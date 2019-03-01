#include "encoding_test.hpp"

#include "storage/create_iterable_from_segment.hpp"

namespace opossum {

class SegmentIteratorsTest : public EncodingTest {};

TEST_P(SegmentIteratorsTest, LegacyForwardIteratorCompatible) {
  /**
   * Test that all (including PointAccess-) Segment iterators can be used as STL LegacyForwardIterators, e.g.,
   * that they can be used in STL algorithms working on sorted ranges.
   */

  const auto table = load_table_with_encoding("resources/test_data/tbl/all_data_types_sorted.tbl", 5);

  /**
   * Takes an iterator pair and verifies its LegacyForwardIterators compability by feeding it into STL algorithms that
   * require LegacyForwardIterators. Most of the testing is that this compiles.
   */
  const auto test_legacy_forward_iterators = [&](const auto begin, const auto end) {
    using ColumnDataType = typename decltype(begin)::ValueType;

    const auto is_sorted = std::is_sorted(begin, end, [](const auto& a, const auto& b) {
      if (a.is_null() && b.is_null()) return false;
      if (a.is_null() && !b.is_null()) return false;
      if (!a.is_null() && b.is_null()) return true;
      return a.value() < b.value();
    });
    ASSERT_TRUE(is_sorted);

    const auto search_value = type_cast<ColumnDataType>(103);
    const auto lower_bound_iter = std::lower_bound(begin, end, search_value, [](const auto& a, const auto& b) {
      if (a.is_null()) return false;
      return a.value() < b;
    });
    ASSERT_NE(lower_bound_iter, end);
    ASSERT_EQ(lower_bound_iter->value(), search_value);
  };

  /**
   * Compose a single-chunk PosList to test the PointAccess iterators.
   */
  auto position_filter = std::make_shared<PosList>();
  position_filter->emplace_back(ChunkID{0}, ChunkOffset{0});
  position_filter->emplace_back(ChunkID{0}, ChunkOffset{1});
  position_filter->emplace_back(ChunkID{0}, ChunkOffset{2});
  position_filter->emplace_back(ChunkID{0}, ChunkOffset{3});
  position_filter->emplace_back(ChunkID{0}, ChunkOffset{4});
  position_filter->guarantee_single_chunk();

  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    const auto base_segment = table->get_chunk(ChunkID{0})->get_segment(column_id);

    resolve_data_and_segment_type(*base_segment, [&](const auto data_type_t, const auto& segment) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      using SegmentType = std::decay_t<decltype(segment)>;

      /**
       * Test the "normal" (sequential) iterators of the Segment
       */
      const auto iterable = create_iterable_from_segment<ColumnDataType, false /* no type erasure */>(segment);
      iterable.with_iterators(test_legacy_forward_iterators);

      /**
       * Test the PointAccess iterators of the Segment
       */
      if constexpr (!std::is_same_v<SegmentType, ReferenceSegment>) {
        iterable.with_iterators(position_filter, test_legacy_forward_iterators);
      }

      /**
       * Test the ReferenceSegment iterators pointing to a single-chunk of the input column
       */
      const auto reference_segment_single_chunk = ReferenceSegment{table, column_id, position_filter};
      const auto reference_segment_single_chunk_iterable =
          ReferenceSegmentIterable<ColumnDataType>(reference_segment_single_chunk);
      reference_segment_single_chunk_iterable.with_iterators(test_legacy_forward_iterators);

      /**
       * Test the ReferenceSegment iterators pointing to multiple chunks of the input column
       */
      const auto position_filter_multi_chunk =
          std::make_shared<PosList>(position_filter->begin(), position_filter->end());
      position_filter_multi_chunk->emplace_back(ChunkID{1}, ChunkOffset{0});
      position_filter_multi_chunk->emplace_back(ChunkID{1}, ChunkOffset{1});
      position_filter_multi_chunk->emplace_back(ChunkID{1}, ChunkOffset{2});
      const auto reference_segment_multi_chunk = ReferenceSegment{table, column_id, position_filter_multi_chunk};
      const auto reference_segment_multi_chunk_iterable =
          ReferenceSegmentIterable<ColumnDataType>(reference_segment_multi_chunk);
      reference_segment_multi_chunk_iterable.with_iterators(test_legacy_forward_iterators);
    });
  }
}

INSTANTIATE_TEST_CASE_P(
    SegmentIteratorsTestInstances, SegmentIteratorsTest,
    ::testing::ValuesIn(std::begin(all_segment_encoding_specs),
                        std::end(all_segment_encoding_specs)), );  // NOLINT(whitespace/parens)  // NOLINT

}  // namespace opossum
