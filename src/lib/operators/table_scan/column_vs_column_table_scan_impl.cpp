#include "column_vs_column_table_scan_impl.hpp"

#include <memory>
#include <string>
#include <type_traits>

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/reference_segment/reference_segment_iterable.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"

namespace opossum {

ColumnVsColumnTableScanImpl::ColumnVsColumnTableScanImpl(const std::shared_ptr<const Table>& in_table,
                                                         const ColumnID left_column_id,
                                                         const PredicateCondition& predicate_condition,
                                                         const ColumnID right_column_id)
    : _in_table(in_table),
      _left_column_id(left_column_id),
      _predicate_condition(predicate_condition),
      _right_column_id{right_column_id} {}

std::string ColumnVsColumnTableScanImpl::description() const { return "ColumnComparison"; }

std::shared_ptr<PosList> ColumnVsColumnTableScanImpl::scan_chunk(ChunkID chunk_id) const {
  const auto chunk = _in_table->get_chunk(chunk_id);

  const auto left_base_segment = chunk->get_segment(_left_column_id);
  const auto right_base_segment = chunk->get_segment(_right_column_id);

  auto matches_out = std::make_shared<PosList>();

  resolve_segment_type(*left_base_segment, [&](const auto& left_segment) {
    using LeftSegmentIterableType = typename decltype(left_it)::IterableType;

    resolve_segment_type(*right_base_segment, [&](const auto& right_segment) {
      using RightSegmentIterableType = typename decltype(right_it)::IterableType;

      using LeftType = typename decltype(left_it)::ValueType;
      using RightType = typename decltype(right_it)::ValueType;

      /**
       * The following generic lambda is instantiated for each combinations of type (int, long, etc.) and
       * each segment iterator type (value, value-non-null, dictionary-simd, ...)!
       * However, not all combinations are valid or possible.
       * Only data segments (value, dictionary) or reference segments will be compared, as a table with both data and
       * reference segments is ruled out. Moreover it is not possible to compare strings to any of the four numerical
       * data types. Therefore, we need to check for these cases and exclude them via the constexpr-if which
       * reduces the number of combinations.
       */

      constexpr auto LEFT_IS_REFERENCE_SEGMENT =
          std::is_same<LeftSegmentIterableType, ReferenceSegmentIterable<LeftType>>{};
      constexpr auto RIGHT_IS_REFERENCE_SEGMENT =
          std::is_same<RightSegmentIterableType, ReferenceSegmentIterable<RightType>>{};

      constexpr auto NEITHER_IS_REFERENCE_SEGMENT = !LEFT_IS_REFERENCE_SEGMENT && !RIGHT_IS_REFERENCE_SEGMENT;
      constexpr auto BOTH_ARE_REFERENCE_SEGMENTS = LEFT_IS_REFERENCE_SEGMENT && RIGHT_IS_REFERENCE_SEGMENT;

      constexpr auto LEFT_IS_STRING_COLUMN = (std::is_same<LeftType, std::string>{});
      constexpr auto RIGHT_IS_STRING_COLUMN = (std::is_same<RightType, std::string>{});

      constexpr auto NEITHER_IS_STRING_COLUMN = !LEFT_IS_STRING_COLUMN && !RIGHT_IS_STRING_COLUMN;
      constexpr auto BOTH_ARE_STRING_COLUMNS = LEFT_IS_STRING_COLUMN && RIGHT_IS_STRING_COLUMN;

      if constexpr ((NEITHER_IS_REFERENCE_SEGMENT || BOTH_ARE_REFERENCE_SEGMENTS) &&
                    (NEITHER_IS_STRING_COLUMN || BOTH_ARE_STRING_COLUMNS)) {
        // Dirty hack to avoid https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86740
        const auto left_it_copy = left_it;
        const auto left_end_copy = left_end;
        const auto right_it_copy = right_it;
        const auto chunk_id_copy = chunk_id;
        const auto& matches_out_copy = matches_out;
        const auto condition = _predicate_condition;

        with_comparator(condition, [&](auto predicate_comparator) {
          auto comparator = [predicate_comparator](const auto& left, const auto& right) {
            return predicate_comparator(left.value(), right.value());
          };
          AbstractTableScanImpl::_scan_with_iterators<true>(comparator, left_it_copy, left_end_copy, chunk_id_copy,
                                                            *matches_out_copy, right_it_copy);
        });
      } else {
        Fail("Invalid segment combination detected!");  // NOLINT - cpplint.py does not know about constexpr
      }
    });
  });

  return matches_out;
}

}  // namespace opossum
