#include "encoding_test.hpp"

#include "storage/create_iterable_from_segment.hpp"

namespace opossum {

class SegmentIteratorsTest : public EncodingTest {

};

TEST_P(SegmentIteratorsTest, LegacyForwardIteratorCompatible) {
  const auto table = load_table_with_encoding("resources/test_data/tbl/int4.tbl", 4);
  ASSERT_EQ(table->row_count(), 4u);
  ASSERT_EQ(table->chunk_count(), 1u);
  ASSERT_EQ(table->column_count(), 1u);

  const auto base_segment = table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});

  resolve_segment_type<int32_t>(*base_segment, [&](const auto& segment) {
    const auto iterable = create_iterable_from_segment<int32_t, false /* no type erasure */>(segment);

    iterable.with_iterators([&](const auto begin, const auto end) {
      const auto is_sorted = std::is_sorted(begin, end, [](const auto& a, const auto& b) {
        return a.value() < b.value();
      });
      ASSERT_TRUE(is_sorted);

      const auto lower_bound_iter = std::lower_bound(begin, end, 1235, [](const auto& a, const auto& b) {
        return a.value() < b;
      });
      ASSERT_NE(lower_bound_iter, end);
      ASSERT_EQ(lower_bound_iter->value(), 12345);
    });
  });
}

INSTANTIATE_TEST_CASE_P(
SegmentIteratorsTestInstances, SegmentIteratorsTest,
::testing::ValuesIn(std::begin(all_segment_encoding_specs),
                    std::end(all_segment_encoding_specs)), );  // NOLINT(whitespace/parens)  // NOLINT


}  // namespace opossum
