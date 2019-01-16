#include "column_vs_column_table_scan_impl.hpp"

#include <memory>
#include <string>
#include <type_traits>

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/reference_segment/reference_segment_iterable.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
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

std::string ColumnVsColumnTableScanImpl::description() const { return "ColumnVsColumn"; }

std::shared_ptr<PosList> ColumnVsColumnTableScanImpl::scan_chunk(ChunkID chunk_id) const {
  const auto chunk = _in_table->get_chunk(chunk_id);
  const auto& left_segment = *chunk->get_segment(_left_column_id);
  const auto& right_segment = *chunk->get_segment(_right_column_id);

  // If the left and the right segment and/or type are not the same, we erase the types even for the release build.
  // This because we have not worked with those combinations and we don't want the templates to be instantiated.
  const bool either_is_reference_segment =
      dynamic_cast<const ReferenceSegment*>(&left_segment) || dynamic_cast<const ReferenceSegment*>(&right_segment);
  // if (typeid(left_segment) == typeid(right_segment) && either_is_reference_segment) {
  //   return _typed_scan_chunk<SegmentIterationTypeErasure::OnlyInDebug>(chunk_id);
  // } else {
    return _typed_scan_chunk<SegmentIterationTypeErasure::Always>(chunk_id);
    if (!HYRISE_DEBUG)
      PerformanceWarning("Using non-specialized (i.e., type-erased) version of ColumnVsColumnTableScan");
  // }
}

template <SegmentIterationTypeErasure type_erasure>
std::shared_ptr<PosList> ColumnVsColumnTableScanImpl::_typed_scan_chunk(ChunkID chunk_id) const {
  const auto chunk = _in_table->get_chunk(chunk_id);

  const auto left_segment = chunk->get_segment(_left_column_id);
  const auto right_segment = chunk->get_segment(_right_column_id);

  auto matches_out = std::make_shared<PosList>();

  auto erase_comparator_type = [](auto comparator, const auto& left_it, const auto& right_it) {
    if constexpr (type_erasure == SegmentIterationTypeErasure::Always) {
      return comparator;
    } else {
      return std::function<bool(const AbstractSegmentPosition<std::decay_t<decltype(left_it->value())>>&,
                                const AbstractSegmentPosition<std::decay_t<decltype(right_it->value())>>&)>{comparator};
    }
  };

  segment_with_iterators<ResolveDataTypeTag, type_erasure>(*left_segment, [&](auto left_it,
                                                                              [[maybe_unused]] const auto left_end) {
    segment_with_iterators<ResolveDataTypeTag, type_erasure>(
        *right_segment, [&](auto right_it, [[maybe_unused]] const auto right_end) {
          using LeftType = typename decltype(left_it)::ValueType;
          using RightType = typename decltype(right_it)::ValueType;

          // C++ cannot compare strings and non-strings out of the box:
          if constexpr (std::is_same_v<LeftType, std::string> == std::is_same_v<RightType, std::string>) {
            bool flipped = false;
            auto condition = _predicate_condition;
            if (condition == PredicateCondition::GreaterThan || condition == PredicateCondition::GreaterThanEquals) {
              condition = flip_predicate_condition(condition);
              flipped = true;
            }

            // Dirty hack to avoid https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86740
            const auto left_it_copy = left_it;
            const auto left_end_copy = left_end;
            const auto right_it_copy = right_it;
            const auto right_end_copy = right_end;
            const auto chunk_id_copy = chunk_id;
            const auto& matches_out_ref = matches_out;

            with_comparator_light(condition, [&](auto predicate_comparator) {
              const auto comparator = [predicate_comparator](const auto& left, const auto& right) {
                return predicate_comparator(left.value(), right.value());
              };

              if (flipped) {
                const auto erased_comparator = erase_comparator_type(comparator, right_it, left_it);
                AbstractTableScanImpl::_scan_with_iterators<true>(erased_comparator, right_it_copy, right_end_copy,
                                                                  chunk_id_copy, *matches_out_ref, left_it_copy);
              } else {
                const auto erased_comparator = erase_comparator_type(comparator, left_it, right_it);
                AbstractTableScanImpl::_scan_with_iterators<true>(erased_comparator, left_it_copy, left_end_copy,
                                                                  chunk_id_copy, *matches_out_ref, right_it_copy);
              }
            });
          } else {
            Fail("Trying to compare strings and non-strings");
          }
        });
  });

  return matches_out;
}

}  // namespace opossum
