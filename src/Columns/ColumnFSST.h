#pragma once

// #ifdef ENABLE_FSST

#include <cstddef>
#include <memory>
#include <vector>

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Columns/IColumn_fwd.h>
#include <DataTypes/Serializations/SerializationStringFSST.h>
#include <base/types.h>
#include <Common/COW.h>
#include <Common/PODArray.h>
#include <Common/WeakHash.h>

#include <fsst_fwd.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
}


class ColumnFSST final : public COWHelper<IColumnHelper<ColumnFSST>, ColumnFSST>
{
    struct ComparatorBase;

    using ComparatorAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorBase>;
    using ComparatorAscendingStable = ComparatorAscendingStableImpl<ComparatorBase>;
    using ComparatorDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorBase>;
    using ComparatorDescendingStable = ComparatorDescendingStableImpl<ComparatorBase>;
    using ComparatorEqual = ComparatorEqualImpl<ComparatorBase>;

private:
    friend class COWHelper<IColumnHelper<ColumnFSST>, ColumnFSST>;
    friend class COW<IColumn>;

    struct BatchDsc
    {
        std::shared_ptr<fsst_decoder_t> decoder;
        size_t batch_start_index;
    };

    WrappedPtr string_column;
    std::vector<UInt64> origin_lengths;
    std::vector<BatchDsc> decoders;
    mutable ColumnPtr decompressed_cache;

    /// Rows with index >= decompressed_start_index are stored uncompressed in string_column.
    /// Rows before this index are FSST-compressed and need a decoder.
    /// SIZE_MAX means no uncompressed tail (all rows are compressed).
    /// 0 means all rows are uncompressed (fully decompressed state).
    size_t decompressed_start_index = SIZE_MAX;

    explicit ColumnFSST(MutableColumnPtr && _string_column)
        : string_column(std::move(_string_column))
    {
    }

    /// Constructs a fully decompressed (empty) ColumnFSST for use by cloneEmpty.
    struct DecompressedTag {};
    explicit ColumnFSST(MutableColumnPtr && _string_column, DecompressedTag)
        : string_column(std::move(_string_column))
        , decompressed_start_index(0)
    {
    }

    ColumnFSST(const ColumnFSST & other)
        : string_column(other.clone())
        , origin_lengths(other.origin_lengths)
        , decoders(other.decoders)
        , decompressed_start_index(other.decompressed_start_index)
    {
    }

    std::optional<size_t> batchByRow(size_t row) const;

    void filterInnerData(const Filter & filt, std::vector<UInt64> & lengths, std::vector<BatchDsc> & decoders) const;


    MutableColumnPtr decompressAll() const;

    /// Decompresses all data into the underlying ColumnString and clears FSST-specific state.
    /// After this call, decompressed_start_index == 0 and all rows are uncompressed.
    void decompressIfNeeded();

    bool hasUncompressedTail() const { return decompressed_start_index != SIZE_MAX; }
    bool isFullyDecompressed() const { return decompressed_start_index == 0; }

    /// Marks the beginning of the uncompressed tail if not already set.
    void ensureUncompressedTail()
    {
        if (!hasUncompressedTail())
            decompressed_start_index = string_column->size();
        decompressed_cache = nullptr;
    }

public:
    using Base = COWHelper<IColumnHelper<ColumnFSST>, ColumnFSST>;
    static Ptr create(const ColumnPtr & nested) { return Base::create(nested->assumeMutable()); }
    static Ptr create(ColumnPtr && nested, std::vector<BatchDsc> decoders, std::vector<UInt64> origin_lengths)
    {
        return Base::create(std::move(nested), std::move(decoders), std::move(origin_lengths));
    }

    ColumnFSST(ColumnPtr && _nested, std::vector<BatchDsc> _decoders, std::vector<UInt64> _origin_lengths)
        : string_column(std::move(_nested))
        , origin_lengths(std::move(_origin_lengths))
        , decoders(std::move(_decoders))
    {
    }

    std::string getName() const override { return string_column->getName(); }
    const char * getFamilyName() const override { return string_column->getFamilyName(); }
    TypeIndex getDataType() const override { return string_column->getDataType(); }

    [[nodiscard]] size_t size() const override { return string_column->size(); }

    [[nodiscard]] bool isFSST() const override { return true; }

    bool structureEquals(const IColumn & rhs) const override
    {
        return typeid(rhs) == typeid(ColumnFSST) || typeid(rhs) == typeid(ColumnString);
    }

    [[nodiscard]] Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    void decompressRow(size_t row_num, String & out) const;

    ColumnPtr convertToFullIfNeeded() const override;

    void getValueNameImpl(WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const override
    {
        auto full = convertToFullIfNeeded();
        full->getValueNameImpl(name_buf, n, options);
    }

    [[nodiscard]] std::string_view getDataAt(size_t n) const override;
    [[nodiscard]] bool isDefaultAt(size_t n) const override;


    void insert(const Field & x) override
    {
        ensureUncompressedTail();
        string_column->insert(x);
    }
    bool tryInsert(const Field & x) override
    {
        ensureUncompressedTail();
        return string_column->tryInsert(x);
    }
    void insertData(const char * pos, size_t length) override
    {
        ensureUncompressedTail();
        string_column->insertData(pos, length);
    }
    void insertDefault() override
    {
        ensureUncompressedTail();
        string_column->insertDefault();
    }
    void deserializeAndInsertFromArena(ReadBuffer & buf, const SerializationSettings * settings) override
    {
        ensureUncompressedTail();
        string_column->deserializeAndInsertFromArena(buf, settings);
    }
    void skipSerializedInArena(ReadBuffer & buf) const override
    {
        getDecompressed();
        decompressed_cache->assumeMutable()->skipSerializedInArena(buf);
    }

    void popBack(size_t n) override;

    MutableColumnPtr cloneEmpty() const override
    {
        return Base::create(ColumnString::create(), DecompressedTag{});
    }

    void updateHashWithValue(size_t n, SipHash & hash) const override;
    WeakHash32 getWeakHash32() const override;
    void updateHashFast(SipHash & hash) const override;

    [[nodiscard]] ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void filter(const Filter & filt) override;

    void expand(const Filter & mask, bool inverted) override
    {
        decompressIfNeeded();
        string_column->expand(mask, inverted);
    }

    [[nodiscard]] ColumnPtr permute(const Permutation & perm, size_t limit) const override { return decompressAll()->permute(perm, limit); }
    [[nodiscard]] ColumnPtr index(const IColumn & indexes, size_t limit) const override { return decompressAll()->index(indexes, limit); }

    void getPermutation(
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res) const override;

    void updatePermutation(
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res,
        EqualRanges & equal_ranges) const override;

    [[nodiscard]] ColumnPtr replicate(const Offsets & offsets) const override;

    void gather(ColumnGathererStream & gatherer_stream) override
    {
        decompressIfNeeded();
        string_column->gather(gatherer_stream);
    }

    [[nodiscard]] size_t byteSize() const override;
    [[nodiscard]] size_t byteSizeAt(size_t) const override;
    [[nodiscard]] size_t allocatedBytes() const override;

    [[nodiscard]] double getRatioOfDefaultRows(double sample_ratio) const override
    {
        return string_column->getRatioOfDefaultRows(sample_ratio);
    }
    [[nodiscard]] UInt64 getNumberOfDefaultRows() const override { return string_column->getNumberOfDefaultRows(); }
    void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const override
    {
        string_column->getIndicesOfNonDefaultRows(indices, from, limit);
    }

    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;

    int doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;

    WrappedPtr getStringColumn() const { return string_column; }
    const std::vector<BatchDsc> & getDecoders() const { return decoders; }
    const std::vector<UInt64> & getLengths() const { return origin_lengths; }

    ColumnPtr getDecompressed() const
    {
        if (!decompressed_cache)
            decompressed_cache = decompressAll();
        return decompressed_cache;
    }

    ColumnPtr createSizeSubcolumn() const;

    void getExtremes(Field & min, Field & max, size_t start, size_t end) const override;

    void append(const CompressedField & x);
    void appendNewBatch(const CompressedField & x, std::shared_ptr<fsst_decoder_t> decoder);

    [[noreturn]] static void throwNotSupported() { throw Exception(ErrorCodes::LOGICAL_ERROR, "functionality is not supported for FSST"); }
};

ColumnPtr recursiveRemoveFSST(const ColumnPtr & column);

};

// #endif
