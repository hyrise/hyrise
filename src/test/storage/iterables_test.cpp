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
      if ((*begin).is_null()) continue;

      _sum += (*begin).value();
    }
  }

  uint32_t& _sum;
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
  }

  std::shared_ptr<Table> table;
  std::shared_ptr<Table> table_with_null;
  std::shared_ptr<Table> table_strings;
};

TEST_F(IterablesTest, ValueSegmentIteratorWithIterators) {
  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_segment(CxlumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueSegment<int>>(column);

  auto iterable = ValueSegmentIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIterator{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, ValueSegmentReferencedIteratorWithIterators) {
  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_segment(CxlumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueSegment<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = ValueSegmentIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(&chunk_offsets, SumUpWithIterator{sum});

  EXPECT_EQ(sum, 12'480u);
}

TEST_F(IterablesTest, ValueSegmentNullableIteratorWithIterators) {
  auto chunk = table_with_null->get_chunk(ChunkID{0u});

  auto column = chunk->get_segment(CxlumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueSegment<int>>(column);

  auto iterable = ValueSegmentIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIterator{sum});

  EXPECT_EQ(sum, 13'702u);
}

TEST_F(IterablesTest, ValueSegmentNullableReferencedIteratorWithIterators) {
  auto chunk = table_with_null->get_chunk(ChunkID{0u});

  auto column = chunk->get_segment(CxlumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueSegment<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = ValueSegmentIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(&chunk_offsets, SumUpWithIterator{sum});

  EXPECT_EQ(sum, 13'579u);
}

TEST_F(IterablesTest, DictionarySegmentIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_segment(CxlumnID{0u});
  auto dict_segment = std::dynamic_pointer_cast<const DictionarySegment<int>>(column);

  auto iterable = DictionarySegmentIterable<int, pmr_vector<int>>{*dict_segment};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIterator{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, DictionarySegmentReferencedIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_segment(CxlumnID{0u});
  auto dict_segment = std::dynamic_pointer_cast<const DictionarySegment<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = DictionarySegmentIterable<int, pmr_vector<int>>{*dict_segment};

  auto sum = uint32_t{0};
  iterable.with_iterators(&chunk_offsets, SumUpWithIterator{sum});

  EXPECT_EQ(sum, 12'480u);
}

TEST_F(IterablesTest, FixedStringDictionarySegmentIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table_strings, EncodingType::FixedStringDictionary);

  auto chunk = table_strings->get_chunk(ChunkID{0u});

  auto column = chunk->get_segment(CxlumnID{0u});
  auto dict_segment = std::dynamic_pointer_cast<const FixedStringDictionarySegment<std::string>>(column);

  auto iterable = DictionarySegmentIterable<std::string, FixedStringVector>{*dict_segment};

  auto concatenate = std::string();
  iterable.with_iterators(AppendWithIterator{concatenate});

  EXPECT_EQ(concatenate, "xxxwwwyyyuuutttzzz");
}

TEST_F(IterablesTest, FixedStringDictionarySegmentReferencedIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table_strings, EncodingType::FixedStringDictionary);

  auto chunk = table_strings->get_chunk(ChunkID{0u});

  auto column = chunk->get_segment(CxlumnID{0u});
  auto dict_segment = std::dynamic_pointer_cast<const FixedStringDictionarySegment<std::string>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = DictionarySegmentIterable<std::string, FixedStringVector>{*dict_segment};

  auto concatenate = std::string();
  iterable.with_iterators(&chunk_offsets, AppendWithIterator{concatenate});

  EXPECT_EQ(concatenate, "xxxyyyuuu");
}

TEST_F(IterablesTest, ReferenceSegmentIteratorWithIterators) {
  auto pos_list =
      PosList{RowID{ChunkID{0u}, 0u}, RowID{ChunkID{0u}, 3u}, RowID{ChunkID{0u}, 1u}, RowID{ChunkID{0u}, 2u}};

  auto reference_segment =
      std::make_unique<ReferenceSegment>(table, CxlumnID{0u}, std::make_shared<PosList>(std::move(pos_list)));

  auto iterable = ReferenceSegmentIterable<int>{*reference_segment};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIterator{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, ValueSegmentIteratorForEach) {
  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_segment(CxlumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueSegment<int>>(column);

  auto iterable = ValueSegmentIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.for_each(SumUp{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, ValueSegmentNullableIteratorForEach) {
  auto chunk = table_with_null->get_chunk(ChunkID{0u});

  auto column = chunk->get_segment(CxlumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueSegment<int>>(column);

  auto iterable = ValueSegmentIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.for_each(SumUp{sum});

  EXPECT_EQ(sum, 13'702u);
}

}  // namespace opossum
