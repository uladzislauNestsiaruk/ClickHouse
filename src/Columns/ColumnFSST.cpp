#ifdef ENABLE_FSST

#    include <algorithm>
#    include <cassert>
#    include <memory>
#    include <optional>
#    include <utility>
#    include <vector>

#    include <Columns/ColumnArray.h>
#    include <Columns/ColumnFSST.h>
#    include <Columns/ColumnMap.h>
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnTuple.h>
#    include <Columns/ColumnsCommon.h>
#    include <Columns/ColumnsNumber.h>
#    include <Core/Field.h>
#    include <base/sanitizer_defs.h>
#    include <base/types.h>
#    include <Common/Exception.h>
#    include <Common/HashTable/Hash.h>
#    include <Common/SipHash.h>
#    include <Common/WeakHash.h>
#    include <Common/assert_cast.h>

#    include <fsst.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int PARAMETER_OUT_OF_BOUND;
}

std::optional<size_t> ColumnFSST::batchByRow(size_t row) const
{
    if (decoders.empty() || decoders[0].batch_start_index > row)
    {
        return std::nullopt;
    }

    auto it = std::upper_bound(
        decoders.begin(), decoders.end(), row, [](size_t value, const BatchDsc & dsc) { return value < dsc.batch_start_index; });
    size_t batch_ind = it == decoders.begin() ? 0 : static_cast<size_t>(--it - decoders.begin());

    return batch_ind;
}

void ColumnFSST::decompressRow(size_t row_num, String & out) const
{
    auto batch_ind = batchByRow(row_num);
    if (!batch_ind.has_value())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "access out of bound");
    }

    auto compressed_data = string_column->getDataAt(row_num);
    const auto & batch_dsc = decoders[batch_ind.value()];

    out.resize(origin_lengths[row_num]);

    fsst_decompress(
        reinterpret_cast<::fsst_decoder_t *>(batch_dsc.decoder.get()),
        compressed_data.size(),
        reinterpret_cast<const unsigned char *>(compressed_data.data()),
        origin_lengths[row_num],
        reinterpret_cast<unsigned char *>(out.data()));
}

Field ColumnFSST::operator[](size_t n) const
{
    if (n >= decompressed_start_index)
        return (*string_column)[n];
    Field result;
    get(n, result);
    return result;
}

void ColumnFSST::get(size_t n, Field & res) const
{
    if (n >= decompressed_start_index)
    {
        string_column->get(n, res);
        return;
    }
    String uncompressed_string(origin_lengths[n], ' ');
    decompressRow(n, uncompressed_string);
    res = std::move(uncompressed_string);
}

void ColumnFSST::appendNewBatch(const CompressedField & x, std::shared_ptr<fsst_decoder_t> decoder)
{
    decoders.emplace_back(BatchDsc{.decoder = decoder, .batch_start_index = string_column->size()});
    append(x);
}
void ColumnFSST::append(const CompressedField & x)
{
    string_column->insert(x.value);
    origin_lengths.push_back(x.uncompressed_size);
}

void ColumnFSST::popBack(size_t n)
{
    if (isFullyDecompressed())
    {
        string_column->popBack(n);
        return;
    }

    size_t current_size = string_column->size();
    size_t new_size = current_size - n;

    string_column->popBack(n);

    if (hasUncompressedTail())
    {
        if (new_size <= decompressed_start_index)
        {
            /// Popped all uncompressed rows and possibly some compressed ones.
            decompressed_start_index = SIZE_MAX;
        }
        else
        {
            /// Still have some uncompressed rows — no changes to compressed metadata needed.
            decompressed_cache = nullptr;
            return;
        }
    }

    /// Trim origin_lengths to match remaining compressed rows.
    while (origin_lengths.size() > new_size)
        origin_lengths.pop_back();
    while (!decoders.empty() && decoders.back().batch_start_index >= origin_lengths.size())
        decoders.pop_back();

    decompressed_cache = nullptr;
}

NO_SANITIZE_UNDEFINED MutableColumnPtr ColumnFSST::decompressAll() const
{
    const size_t n = size();
    if (n == 0)
        return ColumnString::create();

    /// If fully decompressed, just clone the string_column.
    if (isFullyDecompressed())
    {
        auto res = ColumnString::create();
        res->insertRangeFrom(*string_column, 0, n);
        return res;
    }

    auto result = ColumnString::create();
    auto & result_chars = result->getChars();
    auto & result_offsets = result->getOffsets();

    /// Calculate total decompressed size for compressed portion.
    size_t compressed_end = hasUncompressedTail() ? decompressed_start_index : n;

    size_t total_bytes = 0;
    for (size_t i = 0; i < compressed_end; ++i)
        total_bytes += origin_lengths[i];

    /// Also account for uncompressed tail bytes.
    if (hasUncompressedTail())
    {
        const auto & str_col = assert_cast<const ColumnString &>(*string_column);
        const auto & str_offsets = str_col.getOffsets();
        size_t tail_start_offset = decompressed_start_index > 0 ? str_offsets[decompressed_start_index - 1] : 0;
        size_t tail_bytes = str_offsets[n - 1] - tail_start_offset;
        total_bytes += tail_bytes;
    }

    result_chars.resize(total_bytes);
    result_offsets.resize(n);

    /// Decompress compressed batches.
    size_t write_pos = 0;
    for (size_t batch_idx = 0; batch_idx < decoders.size(); ++batch_idx)
    {
        size_t batch_start = decoders[batch_idx].batch_start_index;
        size_t batch_end = (batch_idx + 1 < decoders.size()) ? decoders[batch_idx + 1].batch_start_index : compressed_end;
        auto * decoder = reinterpret_cast<fsst_decoder_t *>(decoders[batch_idx].decoder.get());

        for (size_t row = batch_start; row < batch_end; ++row)
        {
            auto compressed = string_column->getDataAt(row);
            fsst_decompress(
                decoder,
                compressed.size(),
                reinterpret_cast<const unsigned char *>(compressed.data()),
                origin_lengths[row],
                reinterpret_cast<unsigned char *>(&result_chars[write_pos]));
            write_pos += origin_lengths[row];
            result_offsets[row] = write_pos;
        }
    }

    /// Copy uncompressed tail as-is.
    if (hasUncompressedTail())
    {
        for (size_t row = decompressed_start_index; row < n; ++row)
        {
            auto sv = string_column->getDataAt(row);
            memcpy(&result_chars[write_pos], sv.data(), sv.size());
            write_pos += sv.size();
            result_offsets[row] = write_pos;
        }
    }

    return result;
}

ColumnPtr ColumnFSST::convertToFullIfNeeded() const
{
    if (isFullyDecompressed())
        return string_column->getPtr();
    return decompressAll();
}

void ColumnFSST::decompressIfNeeded()
{
    if (isFullyDecompressed())
        return;

    if (decompressed_cache)
    {
        string_column = decompressed_cache->assumeMutable();
    }
    else
    {
        string_column = decompressAll();
    }

    decoders.clear();
    origin_lengths.clear();
    decompressed_cache = nullptr;
    decompressed_start_index = 0;
}

#    if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnFSST::insertRangeFrom(const IColumn & src, size_t start, size_t length)
#    else
void ColumnFSST::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
#    endif
{
    if (src.size() < start + length)
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Parameter out of bound in ColumnFSST::insertRangeFrom method.");

    const auto * src_fsst = typeid_cast<const ColumnFSST *>(&src);
    /// Fast path: both sides are compressed FSST with no uncompressed tail on either side.
    if (src_fsst && !hasUncompressedTail() && !src_fsst->hasUncompressedTail() && !isFullyDecompressed()
        && !src_fsst->isFullyDecompressed())
    {
        size_t length_before_insert = string_column->size();
        string_column->insertRangeFrom(*src_fsst->string_column, start, length);
        origin_lengths.insert(
            origin_lengths.end(), src_fsst->origin_lengths.begin() + start, src_fsst->origin_lengths.begin() + start + length);

        auto first_batch_to_insert = src_fsst->batchByRow(start).value();
        while (first_batch_to_insert < src_fsst->decoders.size()
               && src_fsst->decoders[first_batch_to_insert].batch_start_index < start + length)
        {
            decoders.emplace_back(src_fsst->decoders[first_batch_to_insert]);
            size_t src_batch_start = src_fsst->decoders[first_batch_to_insert].batch_start_index;
            size_t src_offset = (src_batch_start > start) ? (src_batch_start - start) : 0;
            decoders.back().batch_start_index = length_before_insert + src_offset;
            ++first_batch_to_insert;
        }
        return;
    }

    /// Slow path: decompress source rows and append to uncompressed tail.
    ensureUncompressedTail();
    auto decompressed_src = src_fsst ? src_fsst->convertToFullIfNeeded() : src.getPtr();
    string_column->insertRangeFrom(*decompressed_src, start, length);
}

#    if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnFSST::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#    else
int ColumnFSST::doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#    endif
{
    /// If both sides are in uncompressed regions, delegate directly.
    if (isFullyDecompressed())
        return string_column->compareAt(n, m, rhs, nan_direction_hint);

    /// Get lhs value.
    String lhs_val;
    if (n >= decompressed_start_index)
    {
        auto sv = string_column->getDataAt(n);
        lhs_val.assign(sv.data(), sv.size());
    }
    else
    {
        decompressRow(n, lhs_val);
    }

    /// Get rhs value.
    Field rhs_val;
    rhs.get(m, rhs_val);
    const auto & rhs_str = rhs_val.safeGet<String>();

    return memcmpSmallAllowOverflow15(lhs_val.data(), lhs_val.size(), rhs_str.data(), rhs_str.size());
}

size_t ColumnFSST::byteSize() const
{
    if (isFullyDecompressed())
        return string_column->byteSize();
    return string_column->byteSize() + origin_lengths.size() * sizeof(UInt64) + decoders.size() * sizeof(BatchDsc);
}

size_t ColumnFSST::byteSizeAt(size_t n) const
{
    if (isFullyDecompressed() || n >= decompressed_start_index)
        return string_column->byteSizeAt(n);
    return string_column->byteSizeAt(n) + sizeof(origin_lengths[n]);
}

size_t ColumnFSST::allocatedBytes() const
{
    if (isFullyDecompressed())
        return string_column->allocatedBytes();
    return byteSize();
}

ColumnPtr ColumnFSST::createSizeSubcolumn() const
{
    if (isFullyDecompressed())
    {
        const auto & col_str = assert_cast<const ColumnString &>(*string_column);
        auto column_sizes = ColumnUInt64::create();
        size_t rows = col_str.size();
        if (rows == 0)
            return column_sizes;
        auto & sizes_data = column_sizes->getData();
        sizes_data.resize(rows);
        const auto & offsets = col_str.getOffsets();
        for (size_t i = 0; i < rows; ++i)
            sizes_data[i] = offsets[i] - (i > 0 ? offsets[i - 1] : 0) - 1;
        return column_sizes;
    }

    auto column_sizes = ColumnUInt64::create();
    size_t rows = size();
    if (rows == 0)
        return column_sizes;

    auto & sizes_data = column_sizes->getData();
    sizes_data.resize(rows);

    /// Compressed portion uses origin_lengths.
    size_t compressed_end = hasUncompressedTail() ? decompressed_start_index : rows;
    for (size_t i = 0; i < compressed_end; ++i)
        sizes_data[i] = origin_lengths[i];

    /// Uncompressed tail uses string_column offsets.
    if (hasUncompressedTail())
    {
        const auto & col_str = assert_cast<const ColumnString &>(*string_column);
        const auto & offsets = col_str.getOffsets();
        for (size_t i = decompressed_start_index; i < rows; ++i)
            sizes_data[i] = offsets[i] - (i > 0 ? offsets[i - 1] : 0) - 1;
    }

    return column_sizes;
}

std::string_view ColumnFSST::getDataAt(size_t n) const
{
    /// getDataAt always goes through full decompression for consistent string_view lifetime.
    return getDecompressed()->getDataAt(n);
}

bool ColumnFSST::isDefaultAt(size_t n) const
{
    return string_column->isDefaultAt(n);
}

void ColumnFSST::updateHashWithValue(size_t n, SipHash & hash) const
{
    if (n >= decompressed_start_index)
    {
        string_column->updateHashWithValue(n, hash);
        return;
    }
    String decompressed;
    decompressRow(n, decompressed);
    hash.update(decompressed.data(), decompressed.size());
}

WeakHash32 ColumnFSST::getWeakHash32() const
{
    if (isFullyDecompressed())
        return string_column->getWeakHash32();

    WeakHash32 hash(size());
    auto & data = hash.getData();
    size_t n = size();
    for (size_t i = 0; i < n; ++i)
    {
        if (i >= decompressed_start_index)
        {
            /// Use string_column directly for uncompressed rows.
            auto sv = string_column->getDataAt(i);
            data[i] = updateWeakHash32(reinterpret_cast<const UInt8 *>(sv.data()), sv.size(), data[i]);
        }
        else
        {
            String decompressed;
            decompressRow(i, decompressed);
            data[i] = updateWeakHash32(reinterpret_cast<const UInt8 *>(decompressed.data()), decompressed.size(), data[i]);
        }
    }
    return hash;
}

void ColumnFSST::updateHashFast(SipHash & hash) const
{
    if (isFullyDecompressed())
    {
        string_column->updateHashFast(hash);
        return;
    }
    size_t n = size();
    for (size_t i = 0; i < n; ++i)
    {
        if (i >= decompressed_start_index)
        {
            auto sv = string_column->getDataAt(i);
            hash.update(sv.data(), sv.size());
        }
        else
        {
            String decompressed;
            decompressRow(i, decompressed);
            hash.update(decompressed.data(), decompressed.size());
        }
    }
}

void ColumnFSST::filterInnerData(
    const Filter & filt, std::vector<UInt64> & lengths_out, std::vector<BatchDsc> & decoders_out) const // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    size_t dsc_ind = 0;
    for (size_t row = 0; row < std::min(filt.size(), origin_lengths.size()); row++)
    {
        if (!filt[row])
        {
            continue;
        }

        lengths_out.emplace_back(origin_lengths[row]);
        while (dsc_ind < decoders.size() && decoders[dsc_ind].batch_start_index <= row)
        {
            decoders_out.emplace_back(decoders[dsc_ind]);
            decoders_out.back().batch_start_index = lengths_out.size() - 1;
            ++dsc_ind;
        }
    }
}

struct ColumnFSST::ComparatorBase
{
    const ColumnFSST & parent;

    explicit ComparatorBase(const ColumnFSST & parent_)
        : parent(parent_)
    {
    }

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        String lhs_val;
        String rhs_val;

        if (lhs >= parent.decompressed_start_index)
        {
            auto sv = parent.string_column->getDataAt(lhs);
            lhs_val.assign(sv.data(), sv.size());
        }
        else
        {
            parent.decompressRow(lhs, lhs_val);
        }

        if (rhs >= parent.decompressed_start_index)
        {
            auto sv = parent.string_column->getDataAt(rhs);
            rhs_val.assign(sv.data(), sv.size());
        }
        else
        {
            parent.decompressRow(rhs, rhs_val);
        }

        return memcmpSmallAllowOverflow15(lhs_val.data(), lhs_val.size(), rhs_val.data(), rhs_val.size());
    }
};

void ColumnFSST::getPermutation(
    PermutationSortDirection direction, PermutationSortStability stability, size_t limit, int nan_direction_hint, Permutation & res) const
{
    if (isFullyDecompressed())
    {
        string_column->getPermutation(direction, stability, limit, nan_direction_hint, res);
        return;
    }
    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorAscendingUnstable(*this), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorAscendingStable(*this), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorDescendingUnstable(*this), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorDescendingStable(*this), DefaultSort(), DefaultPartialSort());
}

void ColumnFSST::updatePermutation(
    PermutationSortDirection direction,
    PermutationSortStability stability,
    size_t limit,
    int nan_direction_hint,
    Permutation & res,
    EqualRanges & equal_ranges) const
{
    if (isFullyDecompressed())
    {
        string_column->updatePermutation(direction, stability, limit, nan_direction_hint, res, equal_ranges);
        return;
    }

    auto eq_cmp = ComparatorEqual(*this);

    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingUnstable(*this), eq_cmp, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingStable(*this), eq_cmp, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingUnstable(*this), eq_cmp, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingStable(*this), eq_cmp, DefaultSort(), DefaultPartialSort());
}

void ColumnFSST::getExtremes(Field & min, Field & max, size_t start, size_t end) const
{
    if (start >= end)
        return;

    if (isFullyDecompressed())
    {
        string_column->getExtremes(min, max, start, end);
        return;
    }

    min = String();
    max = String();

    size_t min_idx = start;
    size_t max_idx = start;

    ComparatorBase cmp_op(*this);

    for (size_t i = start; i < end; ++i)
    {
        if (cmp_op.compare(i, min_idx) < 0)
            min_idx = i;
        else if (cmp_op.compare(max_idx, i) < 0)
            max_idx = i;
    }

    get(min_idx, min);
    get(max_idx, max);
}


ColumnPtr ColumnFSST::replicate(const Offsets & offsets) const
{
    if (isFullyDecompressed())
        return string_column->replicate(offsets);

    /// If there's an uncompressed tail, decompress everything first.
    if (hasUncompressedTail())
        return decompressAll()->replicate(offsets);

    auto replicated_string_column = string_column->replicate(offsets);

    std::vector<UInt64> replicated_origin_lengths; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<BatchDsc> replicated_decoders; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    replicated_origin_lengths.reserve(replicated_string_column->size());
    replicated_decoders.reserve(decoders.size());

    size_t dsc_ind = 0;
    for (size_t row = 0; row < offsets.size(); row++)
    {
        if (offsets[row] == 0)
            continue;

        while (dsc_ind < decoders.size() && decoders[dsc_ind].batch_start_index <= row)
            ++dsc_ind;

        if (dsc_ind > 0 && dsc_ind <= decoders.size() && decoders[dsc_ind - 1].batch_start_index <= row)
        {
            replicated_decoders.emplace_back(decoders[dsc_ind - 1]);
            replicated_decoders.back().batch_start_index = replicated_origin_lengths.size();
            ++dsc_ind;
        }


        size_t prev = (row == 0 ? 0 : offsets[row - 1]);
        for (size_t ind = 0; ind < offsets[row] - prev; ind++)
            replicated_origin_lengths.emplace_back(origin_lengths[row]);
    }

    return ColumnFSST::create(std::move(replicated_string_column), replicated_decoders, replicated_origin_lengths);
}

ColumnPtr ColumnFSST::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (string_column->empty())
        return cloneEmpty();

    if (isFullyDecompressed())
        return string_column->filter(filt, result_size_hint);

    /// If there's an uncompressed tail, decompress everything first.
    if (hasUncompressedTail())
        return decompressAll()->filter(filt, result_size_hint);

    auto filtered_string_column = string_column->filter(filt, result_size_hint);

    std::vector<BatchDsc> filtered_decoders; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<UInt64> filtered_origin_lengths; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    filtered_origin_lengths.reserve(result_size_hint > 0 ? result_size_hint : string_column->size());
    filterInnerData(filt, filtered_origin_lengths, filtered_decoders);

    return ColumnFSST::create(std::move(filtered_string_column), filtered_decoders, filtered_origin_lengths);
}

void ColumnFSST::filter(const Filter & filt)
{
    if (string_column->empty())
        return;

    if (isFullyDecompressed())
    {
        string_column->filter(filt);
        return;
    }

    /// If there's an uncompressed tail, decompress everything first.
    if (hasUncompressedTail())
    {
        decompressIfNeeded();
        string_column->filter(filt);
        return;
    }

    string_column->filter(filt);

    std::vector<BatchDsc> filtered_decoders; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<UInt64> filtered_origin_lengths; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    filterInnerData(filt, filtered_origin_lengths, filtered_decoders);

    origin_lengths = std::move(filtered_origin_lengths);
    decoders = std::move(filtered_decoders);
}

ColumnPtr recursiveRemoveFSST(const ColumnPtr & column)
{
    if (!column)
        return column;

    if (const auto * column_fsst = typeid_cast<const ColumnFSST *>(column.get()))
    {
        return column_fsst->convertToFullIfNeeded();
    }
    else if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
    {
        auto nested = recursiveRemoveFSST(column_nullable->getNestedColumnPtr());
        if (nested.get() == column_nullable->getNestedColumnPtr().get())
            return column;
        return ColumnNullable::create(nested, column_nullable->getNullMapColumnPtr());
    }
    else if (const auto * column_array = typeid_cast<const ColumnArray *>(column.get()))
    {
        auto data = recursiveRemoveFSST(column_array->getDataPtr());
        if (data.get() == column_array->getDataPtr().get())
            return column;
        return ColumnArray::create(data, column_array->getOffsetsPtr());
    }
    else if (const auto * column_map = typeid_cast<const ColumnMap *>(column.get()))
    {
        auto nested = recursiveRemoveFSST(column_map->getNestedColumnPtr());
        if (nested.get() == column_map->getNestedColumnPtr().get())
            return column;
        return ColumnMap::create(nested);
    }
    else if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        auto columns = column_tuple->getColumns();
        if (columns.empty())
            return column;

        bool changed = false;
        for (auto & element : columns)
        {
            auto new_element = recursiveRemoveFSST(element);
            if (new_element.get() != element.get())
                changed = true;
            element = std::move(new_element);
        }

        if (!changed)
            return column;

        return ColumnTuple::create(columns);
    }

    return column;
}

};

#endif
