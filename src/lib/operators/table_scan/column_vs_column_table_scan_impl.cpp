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

  std::shared_ptr<PosList> result;

  // Reducing the compile time:
  //
  // If the left and the right segment and/or data type are not the same, we erase the segment iterable types even for
  // the release build. For example, ValueSegment<int> == ValueSegment<float> will be erased. So will ValueSegment<int>
  // == DictionarySegment<int>. ReferenceSegments do not need to be handled differently because we expect a table to
  // either have only ReferenceSegments or non-ReferenceSegments.
  //
  // We use type erasure here because we currently do not actively use comparisons between, e.g., a ValueSegment and a
  // DictionarySegment. While it is supported, it is not executed, so we don't want the compiler to spend time
  // instantiating unused templates. Whenever the types of the iterators is removed, we also erase the comparator
  // lambda by wrapping it into an std::function. All of this brought the compile time down by a factor of 5. This
  // is only really relevant for the release build - in the debug build, iterators are erased anyway. Still, we erase
  // the comparator type in the debug build as well.

  resolve_data_and_segment_type(left_segment, [&](auto left_type, auto& left_typed_segment) {
    resolve_data_and_segment_type(right_segment, [&](auto right_type, auto& right_typed_segment) {
      using LeftType = typename decltype(left_type)::type;
      using RightType = typename decltype(right_type)::type;

      if constexpr (!HYRISE_DEBUG && std::is_same_v<decltype(left_typed_segment), decltype(right_typed_segment)>) {
        // Same segment types - do not erase types
        result = _typed_scan_chunk<EraseTypes::OnlyInDebug>(
            chunk_id, create_iterable_from_segment<LeftType>(left_typed_segment),
            create_iterable_from_segment<RightType>(right_typed_segment));
      } else {
        PerformanceWarning("ColumnVsColumnTableScan using type-erased iterators");
        result = _typed_scan_chunk<EraseTypes::Always>(chunk_id, create_any_segment_iterable<LeftType>(left_segment),
                                                       create_any_segment_iterable<RightType>(right_segment));
      }
    });
  });

  return result;
}

template <EraseTypes erase_comparator_type, typename LeftIterable, typename RightIterable>
std::shared_ptr<PosList> ColumnVsColumnTableScanImpl::_typed_scan_chunk(ChunkID chunk_id,
                                                                        const LeftIterable& left_iterable,
                                                                        const RightIterable& right_iterable) const {
  const auto chunk = _in_table->get_chunk(chunk_id);

  auto matches_out = std::make_shared<PosList>();

  using LeftType = typename LeftIterable::ValueType;
  using RightType = typename RightIterable::ValueType;

  // C++ cannot compare strings and non-strings out of the box:
  if constexpr (std::is_same_v<LeftType, std::string> == std::is_same_v<RightType, std::string>) {
    bool condition_was_flipped = false;
    auto maybe_flipped_condition = _predicate_condition;
    if (maybe_flipped_condition == PredicateCondition::GreaterThan ||
        maybe_flipped_condition == PredicateCondition::GreaterThanEquals) {
      maybe_flipped_condition = flip_predicate_condition(maybe_flipped_condition);
      condition_was_flipped = true;
    }

    auto conditionally_erase_comparator_type = [](auto comparator, const auto& it1, const auto& it2) {
      if constexpr (erase_comparator_type == EraseTypes::OnlyInDebug) {
        return comparator;
      } else {
        return std::function<bool(const AbstractSegmentPosition<std::decay_t<decltype(it1->value())>>&,
                                  const AbstractSegmentPosition<std::decay_t<decltype(it2->value())>>&)>{comparator};
      }
    };

    // Dirty hack to avoid https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86740
    const auto chunk_id_copy = chunk_id;
    const auto& matches_out_ref = matches_out;

    left_iterable.with_iterators([&](auto left_it, const auto left_end) {
      right_iterable.with_iterators([&](auto right_it, const auto right_end) {
        with_comparator_light(maybe_flipped_condition, [&](auto predicate_comparator) {
          const auto comparator = [predicate_comparator](const auto& left, const auto& right) {
            return predicate_comparator(left.value(), right.value());
          };

          if (condition_was_flipped) {
            const auto erased_comparator = conditionally_erase_comparator_type(comparator, right_it, left_it);
            AbstractTableScanImpl::_scan_with_iterators<true>(erased_comparator, right_it, right_end, chunk_id_copy,
                                                              *matches_out_ref, left_it);
          } else {
            const auto erased_comparator = conditionally_erase_comparator_type(comparator, left_it, right_it);
            AbstractTableScanImpl::_scan_with_iterators<true>(erased_comparator, left_it, left_end, chunk_id_copy,
                                                              *matches_out_ref, right_it);
          }
        });
      });
    });
  } else {
    Fail("Trying to compare strings and non-strings");
  }

  return matches_out;
}

}  // namespace opossum
