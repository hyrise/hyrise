#include "column_vs_column_table_scan_impl.hpp"

#include <memory>
#include <string>
#include <type_traits>

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/reference_segment/reference_segment_iterable.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
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

std::string ColumnVsColumnTableScanImpl::description() const { return "ColumnVsColumn"; }

std::shared_ptr<PosList> ColumnVsColumnTableScanImpl::scan_chunk(ChunkID chunk_id) const {
  const auto chunk = _in_table->get_chunk(chunk_id);

  const auto left_segment = chunk->get_segment(_left_column_id);
  const auto right_segment = chunk->get_segment(_right_column_id);

  auto matches_out = std::make_shared<PosList>();

  resolve_data_type(left_segment->data_type(), [&](auto left_type) {
    resolve_data_type(right_segment->data_type(), [&](auto right_type) {
      using LeftType = typename decltype(left_type)::type;
      using RightType = typename decltype(right_type)::type;

      // If the left and the right segment and/or type are not the same, we erase the types even for the release build.
      // This because we have not worked with those combinations and we don't want the templates to be instantiated.
      auto create_iterable = [](auto&& segment, auto type) {
        using Type = typename decltype(type)::type;
        if constexpr (!HYRISE_DEBUG && std::is_same_v<LeftType, RightType>) {
          // Generate the fast, typed code
          return create_iterable_from_segment<Type>(segment);
        } else {
          if (!HYRISE_DEBUG) PerformanceWarning("Using non-specialized code for column vs column comparison");
          return create_any_segment_iterable<Type>(*segment);
        }
      };

      const auto left_segment_iterable = create_iterable(left_segment, left_type);
      const auto right_segment_iterable = create_iterable(right_segment, right_type);

      // C++ cannot compare strings and non-strings out of the box:
      if constexpr(std::is_same_v<LeftType, std::string> == std::is_same_v<RightType, std::string>) {
        left_segment_iterable.with_iterators([&](auto left_it, auto left_end) {
          right_segment_iterable.with_iterators([&](auto right_it, auto right_end) {
            bool flipped = false;
            auto condition = _predicate_condition;
            if (condition == PredicateCondition::GreaterThan || condition == PredicateCondition::GreaterThanEquals) {
              condition = flip_predicate_condition(condition);
              flipped = true;
            }

            with_comparator_light(condition, [&](auto predicate_comparator) {
              auto comparator = [predicate_comparator](const auto& left, const auto& right) {
                return predicate_comparator(left.value(), right.value());
              };
              if (flipped) {
                AbstractTableScanImpl::_scan_with_iterators<true>(comparator, right_it, right_end, chunk_id, *matches_out,
                                                                  left_it);
              } else {
                AbstractTableScanImpl::_scan_with_iterators<true>(comparator, left_it, left_end, chunk_id, *matches_out,
                                                                  right_it);
              }
            });
          });
        });
      } else {
        Fail("Trying to compare strings and non-strings");
      }
    });
  });

  return matches_out;
}

}  // namespace opossum
