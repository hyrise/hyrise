#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/chunk_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/dictionary_segment/dictionary_segment_iterable.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/reference_segment/reference_segment_iterable.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"

namespace opossum {

struct SumUpWithIterator {
  template <typename Iterator>
  void operator()(Iterator begin, Iterator end) const {
    _sum = 0u;

    for (; begin != end; ++begin) {
      _accessed_offsets.emplace_back(begin->chunk_offset());

      if (begin->is_null()) continue;

      _sum += begin->value();
    }
  }

  uint32_t& _sum;
  std::vector<ChunkOffset>& _accessed_offsets;
};

struct SumUp {
  template <typename T>
  void operator()(const T& value) const {
    if (value.is_null()) return;

    _sum += value.value();
  }

  uint32_t& _sum;
};

struct AppendWithIterator {
  template <typename Iterator>
  void operator()(Iterator begin, Iterator end) const {
    _concatenate = "";

    for (; begin != end; ++begin) {
      if ((*begin).is_null()) continue;

      _concatenate += (*begin).value();
    }
  }

  std::string& _concatenate;
};

class IterablesTest : public BaseTest {
 protected:
  void SetUp() override {
    table = load_table("src/test/tables/int_float6.tbl", Chunk::MAX_SIZE);
    table_with_null = load_table("src/test/tables/int_float_with_null.tbl", Chunk::MAX_SIZE);
    table_strings = load_table("src/test/tables/string.tbl", Chunk::MAX_SIZE);

    position_filter = std::make_shared<PosList>(
        PosList{{ChunkID{0}, ChunkOffset{0}}, {ChunkID{0}, ChunkOffset{2}}, {ChunkID{0}, ChunkOffset{3}}});
    position_filter->guarantee_single_chunk();
  }

  std::shared_ptr<Table> table;
  std::shared_ptr<Table> table_with_null;
  std::shared_ptr<Table> table_strings;
  std::shared_ptr<PosList> position_filter;
};

TEST_F(IterablesTest, ValueSegmentIteratorWithIterators) {
  auto chunk = table->get_chunk(ChunkID{0u});

  auto segment = chunk->get_segment(ColumnID{0u});
  auto int_segment = std::dynamic_pointer_cast<const ValueSegment<int>>(segment);

  auto iterable = ValueSegmentIterable<int>{*int_segment};

  auto sum = uint32_t{0};
  auto accessed_offsets = std::vector<ChunkOffset>{};
  iterable.with_iterators(SumUpWithIterator{sum, accessed_offsets});

  EXPECT_EQ(sum, 24'825u);
  EXPECT_EQ(accessed_offsets,
            (std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
}

TEST_F(IterablesTest, ValueSegmentReferencedIteratorWithIterators) {
  auto chunk = table->get_chunk(ChunkID{0u});

  auto segment = chunk->get_segment(ColumnID{0u});
  auto int_segment = std::dynamic_pointer_cast<const ValueSegment<int>>(segment);

  auto iterable = ValueSegmentIterable<int>{*int_segment};

  auto sum = uint32_t{0};
  auto accessed_offsets = std::vector<ChunkOffset>{};
  iterable.with_iterators(position_filter, SumUpWithIterator{sum, accessed_offsets});

  EXPECT_EQ(sum, 12'480u);
  EXPECT_EQ(accessed_offsets, (std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}}));
}

TEST_F(IterablesTest, ValueSegmentNullableIteratorWithIterators) {
  auto chunk = table_with_null->get_chunk(ChunkID{0u});

  auto segment = chunk->get_segment(ColumnID{0u});
  auto int_segment = std::dynamic_pointer_cast<const ValueSegment<int>>(segment);

  auto iterable = ValueSegmentIterable<int>{*int_segment};

  auto sum = uint32_t{0};
  auto accessed_offsets = std::vector<ChunkOffset>{};
  iterable.with_iterators(SumUpWithIterator{sum, accessed_offsets});

  EXPECT_EQ(sum, 13'702u);
  EXPECT_EQ(accessed_offsets,
            (std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
}

TEST_F(IterablesTest, ValueSegmentNullableReferencedIteratorWithIterators) {
  auto chunk = table_with_null->get_chunk(ChunkID{0u});

  auto segment = chunk->get_segment(ColumnID{0u});
  auto int_segment = std::dynamic_pointer_cast<const ValueSegment<int>>(segment);

  auto iterable = ValueSegmentIterable<int>{*int_segment};

  auto sum = uint32_t{0};
  auto accessed_offsets = std::vector<ChunkOffset>{};
  iterable.with_iterators(position_filter, SumUpWithIterator{sum, accessed_offsets});

  EXPECT_EQ(sum, 13'579u);
  EXPECT_EQ(accessed_offsets, (std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}}));
}

TEST_F(IterablesTest, DictionarySegmentIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto chunk = table->get_chunk(ChunkID{0u});

  auto segment = chunk->get_segment(ColumnID{0u});
  auto dict_segment = std::dynamic_pointer_cast<const DictionarySegment<int>>(segment);

  auto iterable = DictionarySegmentIterable<int, pmr_vector<int>>{*dict_segment};

  auto sum = uint32_t{0};
  auto accessed_offsets = std::vector<ChunkOffset>{};
  iterable.with_iterators(SumUpWithIterator{sum, accessed_offsets});

  EXPECT_EQ(sum, 24'825u);
  EXPECT_EQ(accessed_offsets,
            (std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
}

TEST_F(IterablesTest, DictionarySegmentReferencedIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto chunk = table->get_chunk(ChunkID{0u});

  auto segment = chunk->get_segment(ColumnID{0u});
  auto dict_segment = std::dynamic_pointer_cast<const DictionarySegment<int>>(segment);

  auto iterable = DictionarySegmentIterable<int, pmr_vector<int>>{*dict_segment};

  auto sum = uint32_t{0};
  auto accessed_offsets = std::vector<ChunkOffset>{};
  iterable.with_iterators(position_filter, SumUpWithIterator{sum, accessed_offsets});

  EXPECT_EQ(sum, 12'480u);
  EXPECT_EQ(accessed_offsets, (std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}}));
}

TEST_F(IterablesTest, FixedStringDictionarySegmentIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table_strings, EncodingType::FixedStringDictionary);

  auto chunk = table_strings->get_chunk(ChunkID{0u});

  auto segment = chunk->get_segment(ColumnID{0u});
  auto dict_segment = std::dynamic_pointer_cast<const FixedStringDictionarySegment<std::string>>(segment);

  auto iterable = DictionarySegmentIterable<std::string, FixedStringVector>{*dict_segment};

  auto concatenate = std::string();
  iterable.with_iterators(AppendWithIterator{concatenate});

  EXPECT_EQ(concatenate, "xxxwwwyyyuuutttzzz");
}

TEST_F(IterablesTest, FixedStringDictionarySegmentReferencedIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table_strings, EncodingType::FixedStringDictionary);

  auto chunk = table_strings->get_chunk(ChunkID{0u});

  auto segment = chunk->get_segment(ColumnID{0u});
  auto dict_segment = std::dynamic_pointer_cast<const FixedStringDictionarySegment<std::string>>(segment);

  auto iterable = DictionarySegmentIterable<std::string, FixedStringVector>{*dict_segment};

  auto concatenate = std::string();
  iterable.with_iterators(position_filter, AppendWithIterator{concatenate});

  EXPECT_EQ(concatenate, "xxxyyyuuu");
}

TEST_F(IterablesTest, ReferenceSegmentIteratorWithIterators) {
  auto pos_list =
      PosList{RowID{ChunkID{0u}, 0u}, RowID{ChunkID{0u}, 3u}, RowID{ChunkID{0u}, 1u}, RowID{ChunkID{0u}, 2u}};

  auto reference_segment =
      std::make_unique<ReferenceSegment>(table, ColumnID{0u}, std::make_shared<PosList>(std::move(pos_list)));

  auto iterable = ReferenceSegmentIterable<int>{*reference_segment};

  auto sum = uint32_t{0};
  auto accessed_offsets = std::vector<ChunkOffset>{};
  iterable.with_iterators(SumUpWithIterator{sum, accessed_offsets});

  EXPECT_EQ(sum, 24'825u);
  EXPECT_EQ(accessed_offsets,
            (std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
}

TEST_F(IterablesTest, ReferenceSegmentIteratorWithIteratorsReadingParallel) {
  // Ensure that two independant reference segment iterators referencing one chunk use the correct accessor after they
  // have been created with the function: <IterableClass>.with_iterators(<Callback>)

  const auto table = load_table("src/test/tables/int_int.tbl", Chunk::MAX_SIZE);

  auto pos_list = std::make_shared<PosList>(PosList{RowID{ChunkID{0u}, 0u}});
  pos_list->guarantee_single_chunk();

  auto reference_segment_a = std::make_unique<ReferenceSegment>(table, ColumnID{0u}, pos_list);
  auto reference_segment_b = std::make_unique<ReferenceSegment>(table, ColumnID{1u}, pos_list);

  // Iterators are stored in the lambda context to hide their actual type
  std::function<int()> dereference_iterator_a;
  std::function<int()> dereference_iterator_b;

  ReferenceSegmentIterable<int>{*reference_segment_a}.with_iterators(
      [&](auto it, auto end) { dereference_iterator_a = [=]() { return (*it).value(); }; });
  ReferenceSegmentIterable<int>{*reference_segment_b}.with_iterators(
      [&](auto it, auto end) { dereference_iterator_b = [=]() { return (*it).value(); }; });

  // Check values of dereferenced iterators after both iterators have been created
  EXPECT_EQ(dereference_iterator_a(), 12345);
  EXPECT_EQ(dereference_iterator_b(), 1);
}

TEST_F(IterablesTest, ValueSegmentIteratorForEach) {
  auto chunk = table->get_chunk(ChunkID{0u});

  auto segment = chunk->get_segment(ColumnID{0u});
  auto int_segment = std::dynamic_pointer_cast<const ValueSegment<int>>(segment);

  auto iterable = ValueSegmentIterable<int>{*int_segment};

  auto sum = uint32_t{0};
  auto accessed_offsets = std::vector<ChunkOffset>{};
  iterable.for_each(SumUp{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, ValueSegmentNullableIteratorForEach) {
  auto chunk = table_with_null->get_chunk(ChunkID{0u});

  auto segment = chunk->get_segment(ColumnID{0u});
  auto int_segment = std::dynamic_pointer_cast<const ValueSegment<int>>(segment);

  auto iterable = ValueSegmentIterable<int>{*int_segment};

  auto sum = uint32_t{0};
  auto accessed_offsets = (std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}});
  iterable.for_each(SumUp{sum});

  EXPECT_EQ(sum, 13'702u);
}

}  // namespace opossum
