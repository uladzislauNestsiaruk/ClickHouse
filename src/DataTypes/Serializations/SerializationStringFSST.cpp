#include <absl/debugging/stacktrace.h>
#include <absl/debugging/symbolize.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#ifdef ENABLE_FSST

#    include <algorithm>
#    include <cstddef>
#    include <cstring>
#    include <memory>
#    include <optional>
#    include <string_view>

#    include <Columns/ColumnFSST.h>
#    include <Columns/IColumn_fwd.h>
#    include <DataTypes/Serializations/SerializationStringFSST.h>
#    include <IO/WriteBuffer.h>
#    include <base/types.h>
#    include <Common/Exception.h>
#    include <Common/PODArray.h>
#    include <Common/PODArray_fwd.h>
#    include <Common/assert_cast.h>
#    include <Common/typeid_cast.h>

#    pragma GCC diagnostic ignored "-Wunused-parameter"


#    include <fsst.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}


template <>
struct SerializeFSSTState<false> : public ISerialization::SerializeBinaryBulkState
{
    SerializeFSSTState() = default;

    PaddedPODArray<UInt8> chars;
    PaddedPODArray<UInt64> offsets;
};

template <>
struct SerializeFSSTState<true> : public ISerialization::SerializeBinaryBulkState
{
    SerializeFSSTState() = default;

    std::vector<std::string_view> compressed_data;
    std::vector<size_t> origin_lengths;
    fsst_decoder_t decoder;
};

class DeserializeFSSTState : public ISerialization::DeserializeBinaryBulkState
{
public:
    DeserializeFSSTState() = default;
    DeserializeFSSTState(
        PODArray<char8_t> & _chars, PODArray<UInt64> & _offsets, PODArray<UInt64> & _origin_lengths, const ::fsst_decoder_t & _decoder)
        : chars(std::move(_chars))
        , offsets(std::move(_offsets))
        , origin_lengths(std::move(_origin_lengths))
        , current_ind(0)
        , decoder(_decoder)
    {
    }

    std::optional<CompressedField> pop();
    const ::fsst_decoder_t & getDecoder() { return decoder; }
    ~DeserializeFSSTState() override = default;

private:
    PODArray<UInt8> chars;
    PODArray<UInt64> offsets;
    PODArray<UInt64> origin_lengths;
    size_t current_ind;
    fsst_decoder_t decoder;
};

std::optional<CompressedField> DeserializeFSSTState::pop()
{
    if (current_ind >= offsets.size())
    {
        return std::nullopt;
    }

    size_t current_offset = offsets[current_ind];
    size_t compressed_string_size = (current_ind + 1 == offsets.size() ? chars.size() : offsets[current_ind + 1]) - current_offset;
    size_t uncompressed_string_size = origin_lengths[current_ind++];
    auto result
        = CompressedField{.value = Field(&chars[current_offset], compressed_string_size), .uncompressed_size = uncompressed_string_size};
    return result;
}

void serializeState(
    std::shared_ptr<SerializeFSSTState<true>> state, WriteBuffer * fsst_stream, WriteBuffer * offsets_stream, WriteBuffer * data_stream)
{
    size_t strings = state->compressed_data.size();
    size_t total_compressed_size = 0;
    std::vector<size_t> offsets_after_compression;

    for (std::string_view string : state->compressed_data)
    {
        offsets_after_compression.emplace_back(total_compressed_size);
        total_compressed_size += string.size();
    }

    // size_t decoder_size = sizeof(fsst_decoder_t);
    // fsst_stream->write(reinterpret_cast<const char *>(&decoder_size), sizeof(decoder_size));
    fsst_stream->write(reinterpret_cast<const char *>(&state->decoder), sizeof(state->decoder));

    std::string lengths_info;
    std::string offsets_after_compression_info;
    for (auto length : state->origin_lengths)
        lengths_info += std::to_string(length) + " \n";
    for (auto offset : offsets_after_compression)
        offsets_after_compression_info += std::to_string(offset) + " \n";

    LOG_DEBUG(getLogger("fsst logger"), "(compressed) compressed strings = {}", state->origin_lengths.size());
    LOG_DEBUG(getLogger("fsst logger"), "(compressed) offsets after compression = {}", offsets_after_compression_info);
    LOG_DEBUG(getLogger("fsst logger"), "(compressed) origin lengths = {}", lengths_info);

    offsets_stream->write(reinterpret_cast<const char *>(&strings), sizeof(strings));
    offsets_stream->write(reinterpret_cast<const char *>(offsets_after_compression.data()), strings * sizeof(size_t));
    offsets_stream->write(reinterpret_cast<const char *>(state->origin_lengths.data()), strings * sizeof(size_t));

    data_stream->write(reinterpret_cast<const char *>(&total_compressed_size), sizeof(total_compressed_size));
    for (std::string_view string : state->compressed_data)
        data_stream->write(reinterpret_cast<const char *>(string.data()), string.size());

    state->compressed_data.clear();
    state->origin_lengths.clear();

    LOG_DEBUG(getLogger("fsst logger"), "chunk serialized(already compressed)");
}

void serializeState(
    std::shared_ptr<SerializeFSSTState<false>> state, WriteBuffer * fsst_stream, WriteBuffer * offsets_stream, WriteBuffer * data_stream)
{
    size_t strings = state->offsets.size();
    PODArray<UInt64> origin_lengths;
    std::unique_ptr<const unsigned char *[]> string_pointers(new const unsigned char *[strings]);
    bool zero_terminated = false; // not sure

    for (size_t ind = 0; ind < strings; ind++)
    {
        size_t next_offset = ind + 1 == strings ? state->chars.size() : state->offsets[ind + 1];
        origin_lengths.push_back(next_offset - state->offsets[ind]);
        string_pointers[ind] = reinterpret_cast<const unsigned char *>(state->chars.data() + state->offsets[ind]);
    }


    auto * fsst_encoder = fsst_create(strings, reinterpret_cast<size_t *>(origin_lengths.data()), string_pointers.get(), zero_terminated);

    PODArray<char8_t> compressed_data(2 * state->chars.size() + 8);
    PODArray<size_t> compressed_data_lenegths(strings);
    std::unique_ptr<unsigned char *[]> compressed_data_pointers(new unsigned char *[strings]);

    size_t compressed_strings = fsst_compress(
        fsst_encoder,
        strings,
        reinterpret_cast<unsigned long *>(origin_lengths.data()), // NOLINT
        string_pointers.get(),
        compressed_data.size(),
        reinterpret_cast<unsigned char *>(compressed_data.data()),
        compressed_data_lenegths.data(),
        compressed_data_pointers.get());

    LOG_DEBUG(getLogger("fsst logger"), "compressed {} strings", compressed_strings);

    PODArray<size_t> offsets_after_compression(compressed_strings);
    for (size_t ind = 0; ind < compressed_strings; ind++)
        offsets_after_compression[ind] = compressed_data_pointers[ind] - compressed_data_pointers[0];

    size_t total_size_after_compression
        = compressed_data_pointers[compressed_strings - 1] - compressed_data_pointers[0] + compressed_data_lenegths[compressed_strings - 1];

    std::string lengths_info;
    std::string offsets_after_compression_info;
    for (auto length : origin_lengths)
        lengths_info += std::to_string(length) + " \n";
    for (auto offset : offsets_after_compression)
        offsets_after_compression_info += std::to_string(offset) + " \n";

    LOG_DEBUG(getLogger("fsst logger"), "(serialize) compressed strings = {}", compressed_strings);
    LOG_DEBUG(getLogger("fsst logger"), "(serialize) offsets after compression = {}", offsets_after_compression_info);
    LOG_DEBUG(getLogger("fsst logger"), "(serialize) origin lengths = {}", lengths_info);

    //unsigned char decoder_serialized[sizeof(fsst_decoder_t)];
    //size_t serialized_decoder_size = fsst_export(fsst_encoder, decoder_serialized);
    //fsst_stream->write(reinterpret_cast<const char *>(&serialized_decoder_size), sizeof(serialized_decoder_size));
    auto decoder = fsst_decoder(fsst_encoder);
    fsst_stream->write(reinterpret_cast<const char *>(&decoder), sizeof(decoder));

    offsets_stream->write(reinterpret_cast<const char *>(&compressed_strings), sizeof(compressed_strings));
    offsets_stream->write(reinterpret_cast<const char *>(offsets_after_compression.data()), compressed_strings * sizeof(size_t));
    offsets_stream->write(reinterpret_cast<const char *>(origin_lengths.data()), compressed_strings * sizeof(size_t));

    data_stream->write(reinterpret_cast<const char *>(&total_size_after_compression), sizeof(total_size_after_compression));
    data_stream->write(reinterpret_cast<const char *>(compressed_data.data()), total_size_after_compression);

    /* clear compressed state */
    if (compressed_strings != strings)
    {
        state->offsets.erase(state->offsets.begin(), state->offsets.begin() + compressed_strings);
        std::transform(
            state->offsets.begin(),
            state->offsets.end(),
            state->offsets.begin(),
            [&](auto offset) { return offset - (string_pointers[compressed_strings] - string_pointers[0]); });

        state->chars.erase(state->chars.begin(), state->chars.begin() + (string_pointers[compressed_strings] - string_pointers[0]));

        LOG_DEBUG(
            getLogger("fsst logger"),
            "compressed only {} strings from {}, chars.size() = {}, offsets.size() = {}",
            compressed_strings,
            strings,
            state->chars.size(),
            state->offsets.size());
        serializeState(state, fsst_stream, offsets_stream, data_stream);
    }
    else
    {
        state->chars.clear();
        state->offsets.clear();
    }

    LOG_DEBUG(getLogger("fsst logger"), "chunk serialized(compressed now)");
}

size_t deserializeState(
    SerializationStringFSST::DeserializeBinaryBulkStatePtr & state,
    ReadBuffer * fsst_stream,
    ReadBuffer * offsets_stream,
    ReadBuffer * compressed_data_stream)
{
    if (offsets_stream->eof())
        return 0;

    /* read fsst */
    /* unsigned char decoder_buffer[sizeof(fsst_decoder_t)];
    fsst_decoder_t decoder;
    size_t decoder_size;
    size_t decoder_read_bytes = fsst_stream->readBig(reinterpret_cast<char *>(&decoder_size), sizeof(decoder_size));

    if (decoder_size > sizeof(fsst_decoder_t))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "FSST decoder size {} exceeds maximum {}. The data is likely corrupt or was written with an incompatible format.",
            decoder_size,
            sizeof(fsst_decoder_t));
    */
    fsst_decoder_t decoder;
    size_t decoder_read_bytes = fsst_stream->readBig(reinterpret_cast<char *>(&decoder), sizeof(decoder));
    // fsst_import(&decoder, decoder_buffer);

    LOG_DEBUG(getLogger("fsst logger"), "fsst_table[0] = {}, fsst_table[1] = {}, fsst_table[2] = {}", decoder.symbol[0], decoder.symbol[1], decoder.symbol[2]);

    /* read offsets and lengths */
    size_t strings;
    size_t metadata_bytes_read = offsets_stream->readBig(reinterpret_cast<char *>(&strings), sizeof(strings));

    LOG_DEBUG(getLogger("fsst logger"), "going to deserialize {} strings", strings);

    static constexpr size_t MAX_STRINGS_PER_BATCH = 10'000'000;
    if (strings > MAX_STRINGS_PER_BATCH)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "FSST batch claims {} strings, which exceeds the sanity limit of {}. "
            "The data is likely corrupt or was written with an incompatible format.",
            strings,
            MAX_STRINGS_PER_BATCH);

    PODArray<UInt64> offsets(strings);
    PODArray<UInt64> origin_lengths(strings);

    metadata_bytes_read += offsets_stream->readBig(reinterpret_cast<char *>(offsets.data()), sizeof(size_t) * strings);
    metadata_bytes_read += offsets_stream->readBig(reinterpret_cast<char *>(origin_lengths.data()), sizeof(size_t) * strings);

    std::string lengths_info;
    std::string offsets_after_compression_info;
    for (auto length : origin_lengths)
        lengths_info += std::to_string(length) + " \n";
    for (auto offset : offsets)
        offsets_after_compression_info += std::to_string(offset) + " \n";

    LOG_DEBUG(getLogger("fsst logger"), "(deserialize) compressed strings = {}", strings);
    LOG_DEBUG(getLogger("fsst logger"), "(deserialize) offsets after compression = {}", offsets_after_compression_info);
    LOG_DEBUG(getLogger("fsst logger"), "(deserialize) origin lengths = {}", lengths_info);

    /* read compressed strings */
    size_t total_compressed_bytes;
    size_t compressed_bytes_read
        = compressed_data_stream->readBig(reinterpret_cast<char *>(&total_compressed_bytes), sizeof(total_compressed_bytes));

    LOG_DEBUG(getLogger("fsst logger"), "going to deserialize {} bytes", total_compressed_bytes);

    PODArray<char8_t> compressed_bytes(total_compressed_bytes);
    compressed_bytes_read += compressed_data_stream->readBig(reinterpret_cast<char *>(compressed_bytes.data()), total_compressed_bytes);

    std::string compressed_data;
    for(auto c : compressed_bytes) {
        compressed_data += std::to_string(static_cast<int>(c)) + "\n";
    }
    LOG_DEBUG(getLogger("fsst logger"), "compressed data {}", compressed_data);

    state = std::make_shared<DeserializeFSSTState>(compressed_bytes, offsets, origin_lengths, decoder);

    LOG_DEBUG(
        getLogger("fsst logger"), "(deserialize) read {} {} {} bytes", decoder_read_bytes, metadata_bytes_read, compressed_bytes_read);
    return decoder_read_bytes + metadata_bytes_read + compressed_bytes_read;
}
ISerialization::KindStack SerializationStringFSST::getKindStack() const
{
    auto kind_stack = nested->getKindStack();
    kind_stack.push_back(Kind::FSST);
    return kind_stack;
}

const IColumn & SerializationStringFSST::resolveColumn(const IColumn & column, ColumnPtr & holder)
{
    if (const auto * col_fsst = typeid_cast<const ColumnFSST *>(&column))
    {
        holder = col_fsst->convertToFullIfNeeded();
        return *holder;
    }
    return column;
}

void SerializationStringFSST::serializeBinary(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    ColumnPtr holder;
    nested->serializeBinary(resolveColumn(column, holder), row_num, ostr, settings);
}

void SerializationStringFSST::serializeTextEscaped(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    ColumnPtr holder;
    nested->serializeTextEscaped(resolveColumn(column, holder), row_num, ostr, settings);
}

void SerializationStringFSST::serializeTextQuoted(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    ColumnPtr holder;
    nested->serializeTextQuoted(resolveColumn(column, holder), row_num, ostr, settings);
}

void SerializationStringFSST::serializeTextCSV(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    ColumnPtr holder;
    nested->serializeTextCSV(resolveColumn(column, holder), row_num, ostr, settings);
}

void SerializationStringFSST::serializeText(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    ColumnPtr holder;
    nested->serializeText(resolveColumn(column, holder), row_num, ostr, settings);
}

void SerializationStringFSST::serializeTextJSON(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    ColumnPtr holder;
    nested->serializeTextJSON(resolveColumn(column, holder), row_num, ostr, settings);
}

SerializationPtr SerializationStringFSST::SubcolumnCreator::create(const SerializationPtr & string_nested, const DataTypePtr &) const
{
    return std::make_shared<SerializationStringFSST>(string_nested);
}

ColumnPtr SerializationStringFSST::SubcolumnCreator::create(const ColumnPtr & prev) const
{
    return ColumnFSST::create(prev);
}

void SerializationStringFSST::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & /*data*/) const
{
    // fsst stream
    settings.path.push_back(Substream::Fsst);
    callback(settings.path);
    settings.path.pop_back();

    // compressed strings offsets stream
    settings.path.push_back(Substream::FsstOffsets);
    callback(settings.path);
    settings.path.pop_back();

    // compressed strings data stream
    settings.path.push_back(Substream::FsstCompressed);
    settings.path.back().creator = std::make_shared<SubcolumnCreator>(ColumnString::create());
    callback(settings.path);
    settings.path.pop_back();
}

void SerializationStringFSST::serializeBinaryBulkWithMultipleStreams(
    const ColumnString * column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::Fsst);
    auto * fsst_stream = settings.getter(settings.path);
    if (!fsst_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "no FSST stream");

    settings.path.back() = Substream::FsstOffsets;
    auto * offsets_stream = settings.getter(settings.path);
    if (!offsets_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed strings offsets stream");

    settings.path.back() = Substream::FsstCompressed;
    auto * data_stream = settings.getter(settings.path);
    if (!data_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed data stream");
    settings.path.pop_back();

    state = state ? state : std::make_shared<SerializeFSSTState<false>>();
    auto serialize_state = std::static_pointer_cast<SerializeFSSTState<false>>(state);

    for (size_t ind = offset; ind < offset + limit; ind++)
    {
        size_t start_offset = ind == 0 ? 0 : column->getOffsets()[ind - 1];
        size_t end_offset = column->getOffsets()[ind];

        serialize_state->offsets.push_back(serialize_state->chars.size());
        serialize_state->chars.insert(column->getChars().data() + start_offset, column->getChars().data() + end_offset);
        if (serialize_state->chars.size() >= kCompressSize)
            serializeState(serialize_state, fsst_stream, offsets_stream, data_stream);
    }

    if (offset + limit >= column->size() && !serialize_state->chars.empty())
        serializeState(serialize_state, fsst_stream, offsets_stream, data_stream);
}

void SerializationStringFSST::serializeBinaryBulkWithMultipleStreams(
    const ColumnFSST * column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::Fsst);
    auto * fsst_stream = settings.getter(settings.path);
    if (!fsst_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "no FSST stream");

    settings.path.back() = Substream::FsstOffsets;
    auto * offsets_stream = settings.getter(settings.path);
    if (!offsets_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed strings offsets stream");

    settings.path.back() = Substream::FsstCompressed;
    auto * data_stream = settings.getter(settings.path);
    if (!data_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed data stream");
    settings.path.pop_back();

    state = state ? state : std::make_shared<SerializeFSSTState<true>>();
    auto serialize_state = std::static_pointer_cast<SerializeFSSTState<true>>(state);

    size_t state_size = 0;
    auto string_column = column->getStringColumn();
    const auto & origin_lengths = column->getLengths();
    const auto & decoders = column->getDecoders();

    auto decoder_it = std::lower_bound(
        decoders.begin(), decoders.end(), offset, [](const auto & decoder, size_t val) { return decoder.batch_start_index < val; });
    for (size_t ind = offset; ind < offset + limit; ind++)
    {
        while (decoder_it != decoders.end() && decoder_it->batch_start_index < ind)
            ++decoder_it;

        serialize_state->compressed_data.emplace_back(string_column->getDataAt(ind));
        serialize_state->origin_lengths.emplace_back(origin_lengths[ind]);
        state_size += origin_lengths.back();

        if (decoder_it != decoders.end() && decoder_it->batch_start_index == ind)
            serialize_state->decoder = *decoder_it->decoder;

        if (state_size >= kCompressSize)
        {
            state_size = 0;
            serializeState(serialize_state, fsst_stream, offsets_stream, data_stream);
        }
    }

    if (offset + limit >= column->size() && !serialize_state->compressed_data.empty())
        serializeState(serialize_state, fsst_stream, offsets_stream, data_stream);
}

void SerializationStringFSST::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    limit = limit == 0 || limit + offset > column.size() ? column.size() - offset : limit;
    if (const auto * column_string = typeid_cast<const ColumnString *>(&column))
    {
        serializeBinaryBulkWithMultipleStreams(column_string, offset, limit, settings, state);
    }
    else if (const auto * column_fsst = typeid_cast<const ColumnFSST *>(&column))
    {
        serializeBinaryBulkWithMultipleStreams(column_fsst, offset, limit, settings, state);
    }
}

void SerializationStringFSST::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t /*rows_offset*/,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * /* cache */) const
{
    settings.path.push_back(Substream::Fsst);
    auto * fsst_stream = settings.getter(settings.path);
    if (!fsst_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "no FSST stream");

    settings.path.back() = Substream::FsstOffsets;
    auto * offsets_stream = settings.getter(settings.path);
    if (!offsets_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed strings offsets stream");

    settings.path.back() = Substream::FsstCompressed;
    auto * compressed_data_stream = settings.getter(settings.path);
    if (!compressed_data_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed data stream");
    settings.path.pop_back();

    auto & column_fsst = assert_cast<ColumnFSST &>(*column->assumeMutable());

    if (!state && deserializeState(state, fsst_stream, offsets_stream, compressed_data_stream) == 0)
        return;

    auto deserialize_state = std::static_pointer_cast<DeserializeFSSTState>(state);
    std::shared_ptr<fsst_decoder_t> decoder_ptr;

    for (size_t rows_read = 0; limit == 0 || rows_read < limit; ++rows_read)
    {
        auto current_field = deserialize_state->pop();
        if (!current_field.has_value())
        {
            decoder_ptr.reset();
            if (deserializeState(state, fsst_stream, offsets_stream, compressed_data_stream) == 0)
                break;
            deserialize_state = std::static_pointer_cast<DeserializeFSSTState>(state);
            current_field = deserialize_state->pop();
            if (!current_field.has_value())
                break;
        }

        if (!decoder_ptr)
        {
            decoder_ptr = std::make_shared<fsst_decoder_t>(deserialize_state->getDecoder());
            column_fsst.appendNewBatch(current_field.value(), decoder_ptr);
        }
        else
            column_fsst.append(current_field.value());
    }
}

};

#endif
