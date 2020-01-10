
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/chunk_encoder.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/dictionary_segment/dictionary_segment_iterable.hpp"
#include "storage/encoding_type.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/lz4_segment/lz4_segment_iterable.hpp"
#include "storage/reference_segment/reference_segment_iterable.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"

namespace opossum {

struct SumUpWithIterator {
  template <typename Iterator>
  void operator()(Iterator begin, Iterator end) const {
    auto distance = end - begin;

    _sum = 0u;

    for (; begin != end; ++begin) {
      --distance;

      _accessed_offsets.emplace_back(begin->chunk_offset());

      if (begin->is_null()) continue;

      _sum += begin->value();
    }

    ASSERT_EQ(distance, 0);
  }

  uint32_t& _sum;
  std::vector<ChunkOffset>& _accessed_offsets;
};

struct CountNullsWithIterator {
  template <typename Iterator>
  void operator()(Iterator begin, Iterator end) const {
    _nulls = 0u;

    for (; begin != end; ++begin) {
      _accessed_offsets.emplace_back(begin->chunk_offset());

      if (begin->is_null()) _nulls++;
    }
  }

  uint32_t& _nulls;
  std::vector<ChunkOffset>& _accessed_offsets;
};

struct SumUp {
  template <typename T>
  void operator()(const T& position) const {
    if (position.is_null()) return;

    _sum += position.value();
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

  pmr_string& _concatenate;
};

class IterablesTest : public BaseTest {
 protected:
  void SetUp() override {
    table = load_table("resources/test_data/tbl/int_float6.tbl");
    table_with_null = load_table("resources/test_data/tbl/int_float_with_null.tbl");
    table_strings = load_table("resources/test_data/tbl/string.tbl");

    position_filter = std::make_shared<PosList>(
        PosList{{ChunkID{0}, ChunkOffset{0}}, {ChunkID{0}, ChunkOffset{2}}, {ChunkID{0}, ChunkOffset{3}}});
    position_filter->guarantee_single_chunk();
  }

  std::shared_ptr<Table> table;
  std::shared_ptr<Table> table_with_null;
  std::shared_ptr<Table> table_strings;
  std::shared_ptr<PosList> position_filter;
};

class EncodedSegmentIterablesTest : public IterablesTest,
                                    public ::testing::WithParamInterface<std::tuple<SegmentEncodingSpec, bool, bool>> {
 public:
  uint32_t expected_sum(bool nullable, bool with_position_filter) {
    if (nullable && with_position_filter) {
      return 12480u;
    } else if (nullable && !with_position_filter) {
      return 24825u;
    } else if (!nullable && with_position_filter) {
      return 13579u;
    } else {
      return 13702u;
    }
  }

  std::vector<ChunkOffset> expected_offsets(bool with_position_filter) {
    if (with_position_filter) {
      return std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}};
    } else {
      return std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}};
    }
  }
};

auto formatter = [](const ::testing::TestParamInfo<std::tuple<SegmentEncodingSpec, bool, bool>> info) {
  auto stream = std::stringstream{};
  stream << std::get<0>(info.param) << (std::get<1>(info.param) ? "WithNulls" : "")
         << (std::get<2>(info.param) ? "WithFilter" : "");

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(SegmentEncoding, EncodedSegmentIterablesTest,
                         ::testing::Combine(::testing::ValuesIn(all_segment_encoding_specs),
                                            ::testing::Bool(),   // nullable
                                            ::testing::Bool()),  // position filter
                         formatter);

TEST_P(EncodedSegmentIterablesTest, IteratorWithIterators) {
  auto encoding_spec = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());
  auto with_position_filter = std::get<2>(GetParam());
  std::shared_ptr<Table> test_table = (nullable ? table : table_with_null);

  auto chunk_encoding_spec = ChunkEncodingSpec{test_table->column_count(), EncodingType::Unencoded};
  for (auto column_id = ColumnID{0}; column_id < test_table->column_count(); ++column_id) {
    if (encoding_supports_data_type(encoding_spec.encoding_type, test_table->column_data_type(column_id))) {
      chunk_encoding_spec[column_id] = encoding_spec;
    }
  }
  ChunkEncoder::encode_all_chunks(test_table, chunk_encoding_spec);

  auto chunk = test_table->get_chunk(ChunkID{0u});
  auto base_segment = chunk->get_segment(ColumnID{0u});

  resolve_data_and_segment_type(*base_segment, [&](const auto data_type_t, const auto& segment) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    using SegmentType = std::decay_t<decltype(segment)>;

    if constexpr (!std::is_same_v<pmr_string, ColumnDataType>) {
      auto sum = uint32_t{0};
      auto accessed_offsets = std::vector<ChunkOffset>{};
      auto functor = SumUpWithIterator{sum, accessed_offsets};

      const auto iterable = create_iterable_from_segment<ColumnDataType, false /* no type erasure */>(segment);
      if (with_position_filter) {
        if constexpr (!std::is_same_v<SegmentType, ReferenceSegment>) {
          iterable.with_iterators(position_filter, functor);
        }
      } else {
        iterable.with_iterators(functor);
      }

      EXPECT_EQ(sum, expected_sum(nullable, with_position_filter));
      EXPECT_EQ(accessed_offsets, expected_offsets(with_position_filter));
    }
  });
}

class EncodedStringSegmentIterablesTest : public IterablesTest,
                                          public ::testing::WithParamInterface<std::tuple<SegmentEncodingSpec, bool>> {
 public:
  pmr_string expected_concatenation(bool with_position_filter) {
    if (with_position_filter) {
      return "xxxyyyuuu";
    } else {
      return "xxxwwwyyyuuutttzzz";
    }
  }
};

auto formatter_string = [](const ::testing::TestParamInfo<std::tuple<SegmentEncodingSpec, bool>> info) {
  auto stream = std::stringstream{};
  stream << std::get<0>(info.param) << "String" << (std::get<1>(info.param) ? "WithFilter" : "");

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(SegmentEncoding, EncodedStringSegmentIterablesTest,
                         ::testing::Combine(::testing::ValuesIn(all_segment_encoding_specs),
                                            ::testing::Bool()),  // position filter
                         formatter_string);

TEST_P(EncodedStringSegmentIterablesTest, IteratorWithIterators) {
  std::shared_ptr<Table> test_table = table_strings;
  auto encoding_spec = std::get<0>(GetParam());
  auto with_position_filter = std::get<1>(GetParam());

  auto chunk_encoding_spec = ChunkEncodingSpec{test_table->column_count(), EncodingType::Unencoded};
  for (auto column_id = ColumnID{0}; column_id < test_table->column_count(); ++column_id) {
    if (encoding_supports_data_type(encoding_spec.encoding_type, test_table->column_data_type(column_id))) {
      chunk_encoding_spec[column_id] = encoding_spec;
    }
  }
  ChunkEncoder::encode_all_chunks(test_table, chunk_encoding_spec);

  auto chunk = test_table->get_chunk(ChunkID{0u});
  auto base_segment = chunk->get_segment(ColumnID{0u});

  resolve_data_and_segment_type(*base_segment, [&](const auto data_type_t, const auto& segment) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    using SegmentType = std::decay_t<decltype(segment)>;

    auto concatenate = pmr_string();
    auto functor = AppendWithIterator{concatenate};

    const auto iterable = create_iterable_from_segment<ColumnDataType, false /* no type erasure */>(segment);
    if (with_position_filter) {
      if constexpr (!std::is_same_v<SegmentType, ReferenceSegment>) {
        iterable.with_iterators(position_filter, functor);
      }
    } else {
      iterable.with_iterators(functor);
    }

    EXPECT_EQ(concatenate, expected_concatenation(with_position_filter));
  });
}

class EncodedSegmentChunkOffsetTest : public IterablesTest,
                                          public ::testing::WithParamInterface<SegmentEncodingSpec> {};

auto formatter_chunk_offset = [](const ::testing::TestParamInfo<SegmentEncodingSpec> info) {
  auto stream = std::stringstream{};
  stream << info.param;

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(SegmentEncoding, EncodedSegmentChunkOffsetTest,
                         ::testing::ValuesIn(all_segment_encoding_specs),
                         formatter_chunk_offset);

TEST_P(EncodedSegmentChunkOffsetTest, IteratorWithIterators){
  auto test_table = table;
  auto encoding_spec = GetParam();
  auto chunk_encoding_spec = ChunkEncodingSpec{test_table->column_count(), EncodingType::Unencoded};
  for (auto column_id = ColumnID{0}; column_id < test_table->column_count(); ++column_id) {
    if (encoding_supports_data_type(encoding_spec.encoding_type, test_table->column_data_type(column_id))) {
      chunk_encoding_spec[column_id] = encoding_spec;
    }
  }
  ChunkEncoder::encode_all_chunks(test_table, chunk_encoding_spec);

  auto chunk = test_table->get_chunk(ChunkID{0u});
  auto base_segment = chunk->get_segment(ColumnID{0u});

  resolve_data_and_segment_type(*base_segment, [&](const auto data_type_t, const auto& segment) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto iterable = create_iterable_from_segment<ColumnDataType, false /* no type erasure */>(segment);

    iterable.with_iterators([&](auto begin, auto end){
        while (begin != end) {
          end--;
        }
        EXPECT_EQ(end->chunk_offset(), 0u);
      }
    );
  });
}

TEST_F(IterablesTest, ReferenceSegmentIteratorWithIterators) {
  auto pos_list = PosList{RowID{ChunkID{0u}, 0u}, RowID{ChunkID{0u}, 3u}, RowID{ChunkID{0u}, 1u},
                          RowID{ChunkID{0u}, 2u}, NULL_ROW_ID};

  auto reference_segment =
      std::make_unique<ReferenceSegment>(table, ColumnID{0u}, std::make_shared<PosList>(std::move(pos_list)));

  auto iterable = ReferenceSegmentIterable<int, EraseReferencedSegmentType::No>{*reference_segment};

  auto sum = uint32_t{0};
  auto accessed_offsets = std::vector<ChunkOffset>{};
  iterable.with_iterators(SumUpWithIterator{sum, accessed_offsets});

  EXPECT_EQ(sum, 24825u);
  EXPECT_EQ(accessed_offsets,
            (std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}, ChunkOffset{4}}));
}

TEST_F(IterablesTest, ReferenceSegmentIteratorWithIteratorsSingleChunk) {
  auto pos_list = PosList{NULL_ROW_ID, NULL_ROW_ID};
  pos_list.guarantee_single_chunk();

  auto reference_segment =
      std::make_unique<ReferenceSegment>(table, ColumnID{0u}, std::make_shared<PosList>(std::move(pos_list)));

  auto iterable = ReferenceSegmentIterable<int, EraseReferencedSegmentType::No>{*reference_segment};

  auto nulls_found = uint32_t{0};
  auto accessed_offsets = std::vector<ChunkOffset>{};
  iterable.with_iterators(CountNullsWithIterator{nulls_found, accessed_offsets});

  EXPECT_EQ(nulls_found, 2u);
  EXPECT_EQ(accessed_offsets, (std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}}));
}

TEST_F(IterablesTest, ReferenceSegmentIteratorWithIteratorsSingleChunkTypeErased) {
  auto pos_list = PosList{NULL_ROW_ID, NULL_ROW_ID};
  pos_list.guarantee_single_chunk();

  auto reference_segment =
      std::make_unique<ReferenceSegment>(table, ColumnID{0u}, std::make_shared<PosList>(std::move(pos_list)));

  auto iterable = ReferenceSegmentIterable<int, EraseReferencedSegmentType::Yes>{*reference_segment};

  auto nulls_found = uint32_t{0};
  auto accessed_offsets = std::vector<ChunkOffset>{};
  iterable.with_iterators(CountNullsWithIterator{nulls_found, accessed_offsets});

  EXPECT_EQ(nulls_found, 2u);
  EXPECT_EQ(accessed_offsets, (std::vector<ChunkOffset>{ChunkOffset{0}, ChunkOffset{1}}));
}

TEST_F(IterablesTest, ValueSegmentIteratorForEach) {
  const auto chunk = table->get_chunk(ChunkID{0u});

  auto segment = chunk->get_segment(ColumnID{0u});
  auto int_segment = std::dynamic_pointer_cast<const ValueSegment<int>>(segment);

  auto iterable = ValueSegmentIterable<int>{*int_segment};

  auto sum = uint32_t{0};
  iterable.for_each(SumUp{sum});

  EXPECT_EQ(sum, 24825u);
}

TEST_F(IterablesTest, ValueSegmentNullableIteratorForEach) {
  const auto chunk = table_with_null->get_chunk(ChunkID{0u});

  auto segment = chunk->get_segment(ColumnID{0u});
  auto int_segment = std::dynamic_pointer_cast<const ValueSegment<int>>(segment);

  auto iterable = ValueSegmentIterable<int>{*int_segment};

  auto sum = uint32_t{0};
  iterable.for_each(SumUp{sum});

  EXPECT_EQ(sum, 13702u);
}

}  // namespace opossum
