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
#    include <Common/SipHash.h>
#    include <Common/assert_cast.h>
#    include <Common/typeid_cast.h>

#    include <fsst.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}

UInt128 SerializationStringFSST::getHash(const SerializationPtr & nested)
{
    SipHash hash;
    hash.update("StringFSST");
    hash.update(nested->getHash());
    return hash.get128();
}

SerializationPtr SerializationStringFSST::create(const SerializationPtr & nested)
{
    if (!nested->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationStringFSST(nested));
    return ISerialization::pooled(getHash(nested), [&] { return new SerializationStringFSST(nested); });
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

struct DeserializeFSSTState : public ISerialization::DeserializeBinaryBulkState
{
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
    const ::fsst_decoder_t & getDecoder() const { return decoder; }
    ~DeserializeFSSTState() override = default;

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

void serializeStates(std::vector<std::shared_ptr<SerializeFSSTState<true>>> states, ISerialization::SerializeBinaryBulkSettings & settings)
{
    using Substream = ISerialization::Substream;

    /* accumulate data */
    std::vector<PODArray<size_t>> offsets_after_compression(states.size());
    std::vector<size_t> total_size_after_compression(states.size());


    for (size_t state_ind = 0; state_ind < states.size(); state_ind++)
    {
        auto & state = states[state_ind];
        for (std::string_view string : state->compressed_data)
        {
            offsets_after_compression[state_ind].emplace_back(total_size_after_compression[state_ind]);
            total_size_after_compression[state_ind] += string.size();
        }
    }

    /* write data */
    settings.path.push_back(Substream::FsstOffsets);
    auto * offsets_stream = settings.getter(settings.path);
    for (size_t state_ind = 0; state_ind < states.size(); state_ind++)
    {
        size_t strings = states[state_ind]->compressed_data.size();
        offsets_stream->write(reinterpret_cast<const char *>(&strings), sizeof(strings));
        offsets_stream->write(reinterpret_cast<const char *>(offsets_after_compression[state_ind].data()), strings * sizeof(size_t));
        offsets_stream->write(reinterpret_cast<const char *>(states[state_ind]->origin_lengths.data()), strings * sizeof(size_t));
    }

    settings.path.back() = Substream::Fsst;
    auto * fsst_stream = settings.getter(settings.path);
    for (auto & state : states)
        fsst_stream->write(reinterpret_cast<const char *>(&state->decoder), sizeof(state->decoder));

    settings.path.back() = Substream::FsstCompressed;
    auto * data_stream = settings.getter(settings.path);
    for (size_t state_ind = 0; state_ind < states.size(); state_ind++)
    {
        data_stream->write(
            reinterpret_cast<const char *>(&total_size_after_compression[state_ind]), sizeof(total_size_after_compression[state_ind]));
        for (std::string_view string : states[state_ind]->compressed_data)
            data_stream->write(reinterpret_cast<const char *>(string.data()), string.size());
    }
    settings.path.pop_back();

    states.clear();
}

void serializeStates(std::vector<std::shared_ptr<SerializeFSSTState<false>>> states, ISerialization::SerializeBinaryBulkSettings & settings)
{
    using Substream = ISerialization::Substream;
    using state_ptr = std::shared_ptr<SerializeFSSTState<false>>;

    /* compress data */
    std::vector<fsst_decoder_t> decoders(states.size());
    std::vector<PODArray<UInt64>> origin_lengths(states.size());
    std::vector<PODArray<size_t>> offsets_after_compression(states.size());
    std::vector<size_t> total_size_after_compression(states.size());
    std::vector<PODArray<char8_t>> compressed_data(states.size());

    size_t max_strings_per_state
        = (*std::max_element(
               states.begin(),
               states.end(),
               [](state_ptr state_a, state_ptr state_b) { return state_b->offsets.size() > state_a->offsets.size(); }))
              ->offsets.size();

    std::unique_ptr<unsigned char *[]> compressed_data_pointers(new unsigned char *[max_strings_per_state]);
    PODArray<size_t> compressed_data_lengths(max_strings_per_state);
    std::unique_ptr<const unsigned char *[]> string_pointers(new const unsigned char *[max_strings_per_state]);
    bool zero_terminated = false; // not sure

    auto compress_state = [&](size_t state_ind)
    {
        const state_ptr & state = states[state_ind];
        size_t strings = state->offsets.size();

        for (size_t ind = 0; ind < strings; ind++)
        {
            size_t next_offset = ind + 1 == strings ? state->chars.size() : state->offsets[ind + 1];
            origin_lengths[state_ind].emplace_back(next_offset - state->offsets[ind]);
            string_pointers[ind] = reinterpret_cast<const unsigned char *>(state->chars.data() + state->offsets[ind]);
        }

        auto * fsst_encoder
            = fsst_create(strings, reinterpret_cast<size_t *>(origin_lengths[state_ind].data()), string_pointers.get(), zero_terminated);

        compressed_data[state_ind].resize(2 * state->chars.size() + 8);
        size_t compressed_strings = fsst_compress(
            fsst_encoder,
            strings,
            reinterpret_cast<unsigned long *>(origin_lengths[state_ind].data()), // NOLINT
            string_pointers.get(),
            compressed_data[state_ind].size(),
            reinterpret_cast<unsigned char *>(compressed_data[state_ind].data()),
            compressed_data_lengths.data(),
            compressed_data_pointers.get());

        offsets_after_compression[state_ind].resize(state->offsets.size());
        for (size_t ind = 0; ind < compressed_strings; ind++)
            offsets_after_compression[state_ind][ind] = compressed_data_pointers[ind] - compressed_data_pointers[0];

        total_size_after_compression[state_ind] = compressed_data_pointers[compressed_strings - 1] - compressed_data_pointers[0]
            + compressed_data_lengths[compressed_strings - 1];

        decoders[state_ind] = fsst_decoder(fsst_encoder);
        fsst_destroy(fsst_encoder);
    };

    for (size_t state_ind = 0; state_ind < states.size(); state_ind++)
        compress_state(state_ind);


    /* write compressed strings offsets and lengths */
    settings.path.push_back(Substream::FsstOffsets);
    auto * offsets_stream = settings.getter(settings.path);
    for (size_t state_ind = 0; state_ind < states.size(); state_ind++)
    {
        size_t compressed_strings = states[state_ind]->offsets.size();
        offsets_stream->write(reinterpret_cast<const char *>(&compressed_strings), sizeof(compressed_strings));
        offsets_stream->write(
            reinterpret_cast<const char *>(offsets_after_compression[state_ind].data()), compressed_strings * sizeof(size_t));
        offsets_stream->write(reinterpret_cast<const char *>(origin_lengths[state_ind].data()), compressed_strings * sizeof(size_t));
    }

    /* write fsst tables */
    settings.path.back() = Substream::Fsst;
    auto * fsst_stream = settings.getter(settings.path);
    for (const auto & decoder : decoders)
        fsst_stream->write(reinterpret_cast<const char *>(&decoder), sizeof(decoder));

    /* write compressed data */
    settings.path.back() = Substream::FsstCompressed;
    auto * data_stream = settings.getter(settings.path);
    for (size_t state_ind = 0; state_ind < states.size(); state_ind++)
    {
        data_stream->write(
            reinterpret_cast<const char *>(&total_size_after_compression[state_ind]), sizeof(total_size_after_compression[state_ind]));
        data_stream->write(reinterpret_cast<const char *>(compressed_data[state_ind].data()), total_size_after_compression[state_ind]);
    }
    settings.path.pop_back();

    states.clear();
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
    return column; // NOLINT(bugprone-return-const-ref-from-parameter)
}

void SerializationStringFSST::serializeBinary(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const
{
    String out;
    assert_cast<const ColumnFSST &>(column).decompressRow(row_num, out);
    writeVarUInt(out.size(), ostr);
    writeString(out, ostr);
}

void SerializationStringFSST::serializeTextEscaped(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const
{
    String out;
    assert_cast<const ColumnFSST &>(column).decompressRow(row_num, out);
    writeEscapedString(out, ostr);
}

void SerializationStringFSST::serializeTextQuoted(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    String out;
    assert_cast<const ColumnFSST &>(column).decompressRow(row_num, out);
    settings.values.escape_quote_with_quote ? writeQuotedStringPostgreSQL(out, ostr) : writeQuotedString(out, ostr);
}

void SerializationStringFSST::serializeTextCSV(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const
{
    String out;
    assert_cast<const ColumnFSST &>(column).decompressRow(row_num, out);
    writeCSVString(out, ostr);
}

void SerializationStringFSST::serializeText(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /* settings */) const
{
    String out;
    assert_cast<const ColumnFSST &>(column).decompressRow(row_num, out);
    writeString(out, ostr);
}

void SerializationStringFSST::serializeTextJSON(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    String out;
    assert_cast<const ColumnFSST &>(column).decompressRow(row_num, out);
    writeJSONString(out, ostr, settings);
}

SerializationPtr SerializationStringFSST::SubcolumnCreator::create(const SerializationPtr & string_nested, const DataTypePtr &) const
{
    return SerializationStringFSST::create(string_nested);
}

ColumnPtr SerializationStringFSST::SubcolumnCreator::create(const ColumnPtr & prev) const
{
    return ColumnFSST::create(prev);
}

void SerializationStringFSST::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    /// FSST does not support reading the `.size` subcolumn independently from disk
    /// because its on-disk format (FsstOffsets/Fsst/FsstCompressed) is incompatible
    /// with SerializationStringSize which expects Regular or StringSizes streams.
    /// Queries like `length(s)` will read the full column and compute sizes.
    (void)data;

    // compressed strings offsets stream (must match serializeStates order)
    settings.path.push_back(Substream::FsstOffsets);
    callback(settings.path);
    settings.path.pop_back();

    // fsst decoder stream
    settings.path.push_back(Substream::Fsst);
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
    state = state ? state : std::make_shared<SerializeFSSTState<false>>();
    auto serialize_state = std::static_pointer_cast<SerializeFSSTState<false>>(state);
    std::vector<decltype(serialize_state)> states_to_serialize;

    serialize_state->chars.clear();
    serialize_state->offsets.clear();

    for (size_t ind = offset; ind < offset + limit; ind++)
    {
        size_t start_offset = ind == 0 ? 0 : column->getOffsets()[ind - 1];
        size_t end_offset = column->getOffsets()[ind];

        serialize_state->offsets.emplace_back(serialize_state->chars.size());
        serialize_state->chars.insert(column->getChars().data() + start_offset, column->getChars().data() + end_offset);
        if (serialize_state->chars.size() >= kCompressSize)
        {
            states_to_serialize.emplace_back(serialize_state);
            serialize_state = std::make_shared<SerializeFSSTState<false>>();
        }
    }

    if (!serialize_state->offsets.empty())
        states_to_serialize.emplace_back(serialize_state);

    if (!states_to_serialize.empty())
        serializeStates(states_to_serialize, settings);
}

void SerializationStringFSST::serializeBinaryBulkWithMultipleStreams(
    const ColumnFSST * column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    state = state ? state : std::make_shared<SerializeFSSTState<true>>();
    auto serialize_state = std::static_pointer_cast<SerializeFSSTState<true>>(state);
    std::vector<decltype(serialize_state)> states_to_serialize;

    size_t state_size = 0;
    auto string_column = column->getStringColumn();
    const auto & origin_lengths = column->getLengths();
    const auto & decoders = column->getDecoders();

    auto decoder_it = std::lower_bound(
        decoders.begin(), decoders.end(), offset, [](const auto & decoder, size_t val) { return decoder.batch_start_index < val; });
    if (decoder_it != decoders.begin())
        --decoder_it;

    for (size_t ind = offset; ind < offset + limit; ind++)
    {
        while (decoder_it != decoders.end() && decoder_it->batch_start_index < ind)
            ++decoder_it;

        serialize_state->compressed_data.emplace_back(string_column->getDataAt(ind));
        serialize_state->origin_lengths.emplace_back(origin_lengths[ind]);
        state_size += serialize_state->origin_lengths.back();

        if (decoder_it != decoders.end() && decoder_it->batch_start_index == ind)
            serialize_state->decoder = *decoder_it->decoder;

        if (state_size >= kCompressSize)
        {
            auto prev_decoder = serialize_state->decoder;
            states_to_serialize.emplace_back(serialize_state);
            serialize_state = std::make_shared<SerializeFSSTState<true>>();
            serialize_state->decoder = prev_decoder;
            state_size = 0;
        }
    }

    if (!serialize_state->origin_lengths.empty())
        states_to_serialize.emplace_back(serialize_state);

    if (!states_to_serialize.empty())
        serializeStates(states_to_serialize, settings);
}

void SerializationStringFSST::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    limit = limit == 0 || limit + offset > column.size() ? column.size() - offset : limit;
    if (limit == 0)
    {
        settings.path.push_back(Substream::FsstOffsets);
        settings.getter(settings.path);
        settings.path.back() = Substream::Fsst;
        settings.getter(settings.path);
        settings.path.back() = Substream::FsstCompressed;
        settings.getter(settings.path);
        settings.path.pop_back();
        return;
    }

    if (const auto * column_string = typeid_cast<const ColumnString *>(&column))
        serializeBinaryBulkWithMultipleStreams(column_string, offset, limit, settings, state);
    else if (const auto * column_fsst = typeid_cast<const ColumnFSST *>(&column))
        serializeBinaryBulkWithMultipleStreams(column_fsst, offset, limit, settings, state);
    else
    {
        auto full_column = column.convertToFullColumnIfConst()->convertToFullColumnIfSparse();
        if (const auto * resolved_string = typeid_cast<const ColumnString *>(full_column.get()))
            serializeBinaryBulkWithMultipleStreams(resolved_string, offset, limit, settings, state);
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "SerializationStringFSST: unexpected column type {}", column.getName());
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
    using Substream = ISerialization::Substream;

    auto & column_fsst = assert_cast<ColumnFSST &>(*column->assumeMutable());
    std::vector<std::shared_ptr<DeserializeFSSTState>> states;
    if (state)
        states.emplace_back(std::static_pointer_cast<DeserializeFSSTState>(state));
    size_t new_states_begin = states.size();

    /* read offsets */
    size_t leftover_rows = 0;
    if (!states.empty())
        leftover_rows = states[0]->offsets.size() - states[0]->current_ind;

    settings.path.push_back(Substream::FsstOffsets);
    auto * offsets_stream = settings.getter(settings.path);
    for (size_t rows_read = leftover_rows; (limit == 0 || rows_read < limit) && !offsets_stream->eof();)
    {
        states.emplace_back(std::make_shared<DeserializeFSSTState>());

        size_t strings;
        (void)offsets_stream->readBig(reinterpret_cast<char *>(&strings), sizeof(strings));

        states.back()->offsets.resize(strings);
        states.back()->origin_lengths.resize(strings);

        (void)offsets_stream->readBig(reinterpret_cast<char *>(states.back()->offsets.data()), sizeof(size_t) * strings);
        (void)offsets_stream->readBig(reinterpret_cast<char *>(states.back()->origin_lengths.data()), sizeof(size_t) * strings);

        rows_read += strings;
    }

    /* read fsst decoders */
    settings.path.back() = Substream::Fsst;
    auto * fsst_stream = settings.getter(settings.path);

    for (size_t ind = new_states_begin; ind < states.size(); ind++)
        (void)fsst_stream->readBig(reinterpret_cast<char *>(&states[ind]->decoder), sizeof(states[ind]->decoder));

    /* read compressed data */
    settings.path.back() = Substream::FsstCompressed;
    auto * compressed_data_stream = settings.getter(settings.path);
    settings.path.pop_back();

    for (size_t ind = new_states_begin; ind < states.size(); ind++)
    {
        size_t total_compressed_bytes;
        (void)compressed_data_stream->readBig(reinterpret_cast<char *>(&total_compressed_bytes), sizeof(total_compressed_bytes));
        states[ind]->chars.resize(total_compressed_bytes);
        (void)compressed_data_stream->readBig(reinterpret_cast<char *>(states[ind]->chars.data()), total_compressed_bytes);
    }

    /* fill Column */
    size_t current_state = 0;
    bool need_new_batch = true;
    for (size_t rows_read = 0; (limit == 0 || rows_read < limit) && current_state < states.size(); ++rows_read)
    {
        auto current_field = states[current_state]->pop();

        if (current_field.has_value())
        {
            if (need_new_batch)
            {
                column_fsst.appendNewBatch(current_field.value(), std::make_shared<fsst_decoder_t>(states[current_state]->getDecoder()));
                need_new_batch = false;
            }
            else
                column_fsst.append(current_field.value());
        }
        else
        {
            ++current_state;
            need_new_batch = true;
            --rows_read;
        }
    }

    if (current_state < states.size())
        state = states[current_state];
}

};

#endif
