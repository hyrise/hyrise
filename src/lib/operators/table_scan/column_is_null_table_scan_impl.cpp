#include "column_is_null_table_scan_impl.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "abstract_dereferenced_column_table_scan_impl.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/base_dictionary_segment.hpp"
#include "storage/base_value_segment.hpp"
#include "storage/frame_of_reference_segment.hpp"
#include "storage/lz4_segment.hpp"
#include "storage/pos_lists/abstract_pos_list.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment/null_value_vector_iterable.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

ColumnIsNullTableScanImpl::ColumnIsNullTableScanImpl(const std::shared_ptr<const Table>& in_table,
                                                     const ColumnID column_id,
                                                     const PredicateCondition& init_predicate_condition)
    : AbstractDereferencedColumnTableScanImpl(in_table, column_id, init_predicate_condition) {
  DebugAssert(predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull,
              "Invalid PredicateCondition");
}

std::string ColumnIsNullTableScanImpl::description() const {
  return "IsNullScan";
}

void ColumnIsNullTableScanImpl::_scan_non_reference_segment(
    const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) {
  resolve_data_type(segment.data_type(), [&](auto type) {
    using SegmentDataType = typename decltype(type)::type;

    // The ColumnIsNullTableScan is optimized for Value, Dictionary, LZ4, and FrameofReference segments since their
    // NULL values can be efficiently iterated through their null_values or attribute vector. RunLength segments use
    // the _scan_generic_segment() method because their NULL values are stored in runs, making iteration less easy.

    if (const auto* typed_segment = dynamic_cast<const BaseValueSegment*>(&segment)) {
      _scan_typed_segment(*typed_segment, chunk_id, matches, position_filter);
    } else if (const auto* typed_segment = dynamic_cast<const BaseDictionarySegment*>(&segment)) {
      _scan_typed_segment(*typed_segment, chunk_id, matches, position_filter);
    } else if (const auto* typed_segment = dynamic_cast<const LZ4Segment<SegmentDataType>*>(&segment)) {
      _scan_typed_segment(*typed_segment, chunk_id, matches, position_filter);
    } else if (const auto* typed_segment = dynamic_cast<const FrameOfReferenceSegment<int32_t>*>(&segment)) {
      _scan_typed_segment(*typed_segment, chunk_id, matches, position_filter);
    } else {
      const auto& chunk_sorted_by = _in_table->get_chunk(chunk_id)->individually_sorted_by();
      if (!chunk_sorted_by.empty()) {
        for (const auto& sorted_by : chunk_sorted_by) {
          if (sorted_by.column == _column_id) {
            _scan_generic_sorted_segment(segment, chunk_id, matches, position_filter, sorted_by.sort_mode);
            ++num_chunks_with_binary_search;
          }
        }
      } else {
        _scan_generic_segment(segment, chunk_id, matches, position_filter);
      }
    }
  });
}

void ColumnIsNullTableScanImpl::_scan_generic_segment(
    const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const {
  segment_with_iterators_filtered(segment, position_filter, [&](const auto& iter, [[maybe_unused]] const auto& end) {
    // This may also be called for a ValueSegment if `segment` is a ReferenceSegment pointing to a single ValueSegment.
    const auto invert = predicate_condition == PredicateCondition::IsNotNull;
    const auto functor = [&](const auto& value) {
      return invert ^ value.is_null();
    };

    _scan_with_iterators<false>(functor, iter, end, chunk_id, matches);
  });
}

void ColumnIsNullTableScanImpl::_scan_generic_sorted_segment(
    const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter, const SortMode sorted_by) const {
  const bool is_nulls_first = sorted_by == SortMode::Ascending || sorted_by == SortMode::Descending;
  const bool predicate_is_null = predicate_condition == PredicateCondition::IsNull;
  segment_with_iterators_filtered(segment, position_filter, [&](auto begin, auto end) {
    if (is_nulls_first) {
      const auto first_not_null =
          std::lower_bound(begin, end, bool{}, [](const auto& segment_position, const auto& /*end*/) {
            return segment_position.is_null();
          });
      if (predicate_is_null) {
        end = first_not_null;
      } else {
        begin = first_not_null;
      }
    } else {
      // NULLs last.
      const auto first_null =
          std::lower_bound(begin, end, bool{}, [](const auto& segment_position, const auto& /*end*/) {
            return !segment_position.is_null();
          });
      if (predicate_is_null) {
        begin = first_null;
      } else {
        end = first_null;
      }
    }

    size_t output_idx = matches.size();
    matches.resize(matches.size() + std::distance(begin, end));
    for (auto segment_it = begin; segment_it != end; ++segment_it) {
      matches[output_idx++] = RowID{chunk_id, segment_it->chunk_offset()};
    }
  });
}

template <typename BaseSegmentType>
void ColumnIsNullTableScanImpl::_scan_typed_segment(const BaseSegmentType& segment, const ChunkID chunk_id,
                                                    RowIDPosList& matches,
                                                    const std::shared_ptr<const AbstractPosList>& position_filter) {
  if (_matches_all(segment)) {
    _add_all(chunk_id, matches, position_filter ? position_filter->size() : segment.size());
    ++num_chunks_with_all_rows_matching;
    return;
  }

  if (_matches_none(segment)) {
    ++num_chunks_with_early_out;
    return;
  }

  // TODO(anyone): Merge the first and third branch in case of harmonized null_values() interfaces.
  if constexpr (std::is_same_v<BaseSegmentType, BaseValueSegment>) {
    DebugAssert(segment.is_nullable(), "Columns that are not nullable should have been caught by edge case handling.");
    _scan_iterable_for_null_values(NullValueVectorIterable{segment.null_values()}, chunk_id, matches, position_filter);
  } else if constexpr (std::is_same_v<BaseSegmentType, BaseDictionarySegment>) {
    DebugAssert(segment.unique_values_count() != 0 && segment.unique_values_count() != segment.size(),
                "DictionarySegments without or with exclusively NULLs should have been caught by edge case handling.");
    _scan_iterable_for_null_values(create_iterable_from_attribute_vector(segment), chunk_id, matches, position_filter);
  } else {
    const auto& null_values = segment.null_values();
    DebugAssert(null_values.has_value(),
                "Segment without null_values vector should have been caught by edge case handling.");
    _scan_iterable_for_null_values(NullValueVectorIterable{*null_values}, chunk_id, matches, position_filter);
  }
}

template <typename BaseIterableType>
void ColumnIsNullTableScanImpl::_scan_iterable_for_null_values(
    const BaseIterableType& iterable, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const {
  const auto invert = predicate_condition == PredicateCondition::IsNotNull;
  const auto functor = [&](const auto& value) {
    return invert ^ value.is_null();
  };

  iterable.with_iterators(position_filter, [&](const auto& iter, const auto& end) {
    _scan_with_iterators<false>(functor, iter, end, chunk_id, matches);
  });
}

template <>
bool ColumnIsNullTableScanImpl::_matches_all(const BaseDictionarySegment& segment) const {
  switch (predicate_condition) {
    case PredicateCondition::IsNull:
      return segment.unique_values_count() == 0;

    case PredicateCondition::IsNotNull:
      // Since DictionarySegments do not use an additional data structure to store their NULL values, we are only sure
      // it contains no NULLs if it only contains unique, non-NULL values.
      return segment.unique_values_count() == segment.size();

    default:
      Fail("Unsupported comparison type encountered.");
  }
}

template <>
bool ColumnIsNullTableScanImpl::_matches_none(const BaseDictionarySegment& segment) const {
  switch (predicate_condition) {
    case PredicateCondition::IsNull:
      return segment.unique_values_count() == segment.size();

    case PredicateCondition::IsNotNull:
      return segment.unique_values_count() == 0;

    default:
      Fail("Unsupported comparison type encountered.");
  }
}

template <>
bool ColumnIsNullTableScanImpl::_matches_all(const BaseValueSegment& segment) const {
  switch (predicate_condition) {
    case PredicateCondition::IsNull:
      return false;

    case PredicateCondition::IsNotNull:
      return !segment.is_nullable();

    default:
      Fail("Unsupported comparison type encountered.");
  }
}

template <>
bool ColumnIsNullTableScanImpl::_matches_none(const BaseValueSegment& segment) const {
  switch (predicate_condition) {
    case PredicateCondition::IsNull:
      return !segment.is_nullable();

    case PredicateCondition::IsNotNull:
      return false;

    default:
      Fail("Unsupported comparison type encountered.");
  }
}

template <typename BaseSegmentType>
bool ColumnIsNullTableScanImpl::_matches_all(const BaseSegmentType& segment) const {
  switch (predicate_condition) {
    case PredicateCondition::IsNull:
      return false;

    case PredicateCondition::IsNotNull:
      return !segment.null_values().has_value();

    default:
      Fail("Unsupported comparison type encountered.");
  }
}

template <typename BaseSegmentType>
bool ColumnIsNullTableScanImpl::_matches_none(const BaseSegmentType& segment) const {
  switch (predicate_condition) {
    case PredicateCondition::IsNull:
      return !segment.null_values().has_value();

    case PredicateCondition::IsNotNull:
      return false;

    default:
      Fail("Unsupported comparison type encountered.");
  }
}

void ColumnIsNullTableScanImpl::_add_all(const ChunkID chunk_id, RowIDPosList& matches, const size_t segment_size) {
  const auto num_rows = segment_size;
  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < num_rows; ++chunk_offset) {
    matches.emplace_back(chunk_id, chunk_offset);
  }
}

}  // namespace hyrise
