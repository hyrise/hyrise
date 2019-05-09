#include "multi_predicate_join_evaluator.hpp"

#include <vector>

#include "operators/operator_join_predicate.hpp"
#include "storage/table.hpp"
#include "type_comparison.hpp"
#include "types.hpp"

namespace opossum {

MultiPredicateJoinEvaluator::MultiPredicateJoinEvaluator(const Table& left, const Table& right,
                                                         const JoinMode join_mode,
                                                         const std::vector<OperatorJoinPredicate>& join_predicates) {
  for (const auto& predicate : join_predicates) {
    resolve_data_type(left.column_data_type(predicate.column_ids.first), [&](auto left_type) {
      resolve_data_type(right.column_data_type(predicate.column_ids.second), [&](auto right_type) {
        using LeftColumnDataType = typename decltype(left_type)::type;
        using RightColumnDataType = typename decltype(right_type)::type;

        // This code has been copied from JoinNestedLoop::_join_two_untyped_segments
        constexpr auto LEFT_IS_STRING_COLUMN = (std::is_same<LeftColumnDataType, pmr_string>{});
        constexpr auto RIGHT_IS_STRING_COLUMN = (std::is_same<RightColumnDataType, pmr_string>{});

        constexpr auto NEITHER_IS_STRING_COLUMN = !LEFT_IS_STRING_COLUMN && !RIGHT_IS_STRING_COLUMN;
        constexpr auto BOTH_ARE_STRING_COLUMNS = LEFT_IS_STRING_COLUMN && RIGHT_IS_STRING_COLUMN;

        if constexpr (NEITHER_IS_STRING_COLUMN || BOTH_ARE_STRING_COLUMNS) {
          auto left_accessors = _create_accessors<LeftColumnDataType>(left, predicate.column_ids.first);
          auto right_accessors = _create_accessors<RightColumnDataType>(right, predicate.column_ids.second);
          auto join_mode_copy = join_mode;

          // We need to do this assignment to work around an internal compiler error.
          // The compiler error would occur, if you tried to directly access _comparators within the following
          // lambda. This error is discussed at https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86740
          auto& comparators = _comparators;

          with_comparator(predicate.predicate_condition, [&](auto comparator) {
            comparators.emplace_back(
                std::make_unique<FieldComparator<decltype(comparator), LeftColumnDataType, RightColumnDataType>>(
                    comparator, join_mode_copy, std::move(left_accessors), std::move(right_accessors)));
          });
        } else {
          Fail("Types of columns cannot be compared.");
        }
      });
    });
  }
}

bool MultiPredicateJoinEvaluator::satisfies_all_predicates(const RowID& left_row_id, const RowID& right_row_id) {
  for (const auto& comparator : _comparators) {
    if (!comparator->compare(left_row_id, right_row_id)) {
      return false;
    }
  }

  return true;
}

template <typename T>
std::vector<std::unique_ptr<AbstractSegmentAccessor<T>>> MultiPredicateJoinEvaluator::_create_accessors(
    const Table& table, const ColumnID column_id) {
  std::vector<std::unique_ptr<AbstractSegmentAccessor<T>>> accessors;
  accessors.resize(table.chunk_count());
  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    const auto& chunk = table.get_chunk(chunk_id);
    // get_chunk might return nullptr if we iterate over chunk_count manually
    // https://github.com/hyrise/hyrise/pull/1482#discussion_r261860186
    if (chunk) {
      const auto& segment = table.get_chunk(chunk_id)->get_segment(column_id);
      accessors[chunk_id] = create_segment_accessor<T>(segment);
    }
  }

  return accessors;
}

}  // namespace opossum
