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
  const auto left_segment = chunk->get_segment(_left_column_id);
  const auto right_segment = chunk->get_segment(_right_column_id);

  std::shared_ptr<PosList> result;

  /**
   * Reducing the compile time:
   *
   * If the left and the right segment and/or data type are not the same, we erase the segment iterable types EVEN for
   * the release build. For example, ValueSegment<int> == ValueSegment<float> will be erased. So will ValueSegment<int>
   * == DictionarySegment<int>.
   *
   * We use type erasure here because we currently do not actively use comparisons between, e.g., a ValueSegment and a
   * DictionarySegment. While it is supported, it is not executed, so we don't want the compiler to spend time
   * instantiating unused templates. Whenever the types of the iterables is removed, we also erase the comparator
   * lambda (in `_typed_scan_chunk`) by wrapping it into an std::function. All of this brought the compile time of
   * this translation unit down significantly. This is only really relevant for the release build - in the debug build,
   * iterables are always erased. The comparator type is being erased in the debug build as well.
   */

  /**
   * FAST PATH
   * ...in which the SegmentType does not get erased in Release builds. DataTypes and SegmentTypes have to be the same.
   * In the case of ReferenceSegments, the used ReferenceSegmentIterator has to be the same (see below).
   */
  if (left_segment->data_type() == right_segment->data_type()) {
    resolve_data_and_segment_type(*left_segment, [&](auto data_type_t, auto& left_typed_segment) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      using SegmentType = std::decay_t<decltype(left_typed_segment)>;

      if (const auto right_typed_segment = std::dynamic_pointer_cast<SegmentType>(right_segment)) {
        if constexpr (std::is_same_v<SegmentType, ReferenceSegment>) {
          // For reference segments check if the iterables end up using the same type of iterator.
          auto left_iterable =
              ReferenceSegmentIterable<ColumnDataType, EraseReferencedSegmentType::No>{left_typed_segment};
          auto right_iterable =
              ReferenceSegmentIterable<ColumnDataType, EraseReferencedSegmentType::No>{*right_typed_segment};

          left_iterable.with_iterators([&](auto left_it, const auto left_end) {
            right_iterable.with_iterators([&](auto right_it, const auto right_end) {
              if constexpr (std::is_same_v<std::decay_t<decltype(left_it)>, std::decay_t<decltype(right_it)>>) {
                // Either both reference segments use the MultipleChunkIterator (which uses erased accessors anyway)
                // or they use a SingleChunkIterator pointing to the same segment type (e.g., Dictionary and Dictionary)
                result = _typed_scan_chunk_with_iterators<EraseTypes::OnlyInDebugBuild>(chunk_id, left_it, left_end,
                                                                                        right_it, right_end);
              }
            });
          });
        } else {
          // Same segment types - do not erase types in Release builds
          result = _typed_scan_chunk_with_iterables<EraseTypes::OnlyInDebugBuild>(
              chunk_id, create_iterable_from_segment<ColumnDataType>(left_typed_segment),
              create_iterable_from_segment<ColumnDataType>(*right_typed_segment));
        }
      }
    });

    // `result` will still be nullptr if the SegmentTypes were not the same - if that's the case we have to take the
    // "slow" path further down to perform the scan
    if (result) {
      return result;
    }
  }

  /**
   * SLOW PATH
   * ...in which the left and right segment iterables are erased into AnySegmentIterables<T>
   */
  resolve_data_type(left_segment->data_type(), [&](const auto left_data_type_t) {
    using LeftColumnDataType = typename decltype(left_data_type_t)::type;

    auto left_iterable = create_any_segment_iterable<LeftColumnDataType>(*left_segment);

    resolve_data_type(right_segment->data_type(), [&](const auto right_data_type_t) {
      using RightColumnDataType = typename decltype(right_data_type_t)::type;

      auto right_iterable = create_any_segment_iterable<RightColumnDataType>(*right_segment);

      PerformanceWarning("ColumnVsColumnTableScan using type-erased iterators");
      result = _typed_scan_chunk_with_iterables<EraseTypes::Always>(chunk_id, left_iterable, right_iterable);
    });
  });

  return result;
}

template <EraseTypes erase_comparator_type, typename LeftIterable, typename RightIterable>
std::shared_ptr<PosList> __attribute__((noinline))
ColumnVsColumnTableScanImpl::_typed_scan_chunk_with_iterables(ChunkID chunk_id, const LeftIterable& left_iterable,
                                                              const RightIterable& right_iterable) const {
  auto matches_out = std::shared_ptr<PosList>{};

  left_iterable.with_iterators([&](auto left_it, const auto left_end) {
    right_iterable.with_iterators([&](auto right_it, const auto right_end) {
      matches_out =
          _typed_scan_chunk_with_iterators<erase_comparator_type>(chunk_id, left_it, left_end, right_it, right_end);
    });
  });

  return matches_out;
}

template <EraseTypes erase_comparator_type, typename LeftIterator, typename RightIterator>
std::shared_ptr<PosList> __attribute__((noinline))
ColumnVsColumnTableScanImpl::_typed_scan_chunk_with_iterators(ChunkID chunk_id, LeftIterator& left_it,
                                                              const LeftIterator& left_end, RightIterator& right_it,
                                                              const RightIterator& right_end) const {
  const auto chunk = _in_table->get_chunk(chunk_id);

  auto matches_out = std::make_shared<PosList>();

  using LeftType = typename LeftIterator::ValueType;
  using RightType = typename RightIterator::ValueType;

  // C++ cannot compare strings and non-strings out of the box:
  if constexpr (std::is_same_v<LeftType, pmr_string> == std::is_same_v<RightType, pmr_string>) {
    bool condition_was_flipped = false;
    auto maybe_flipped_condition = _predicate_condition;
    if (maybe_flipped_condition == PredicateCondition::GreaterThan ||
        maybe_flipped_condition == PredicateCondition::GreaterThanEquals) {
      maybe_flipped_condition = flip_predicate_condition(maybe_flipped_condition);
      condition_was_flipped = true;
    }

    auto conditionally_erase_comparator_type = [](auto comparator, const auto& it1, const auto& it2) {
      if constexpr (erase_comparator_type == EraseTypes::OnlyInDebugBuild) {
        return comparator;
      } else {
        return std::function<bool(const AbstractSegmentPosition<std::decay_t<decltype(it1->value())>>&,
                                  const AbstractSegmentPosition<std::decay_t<decltype(it2->value())>>&)>{comparator};
      }
    };

    // Dirty hack to avoid https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86740
    const auto chunk_id_copy = chunk_id;
    const auto& matches_out_ref = matches_out;

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
  } else {
    Fail("Trying to compare strings and non-strings");
  }

  return matches_out;
}

}  // namespace opossum
