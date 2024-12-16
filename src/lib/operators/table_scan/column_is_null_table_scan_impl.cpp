#include "column_is_null_table_scan_impl.hpp"

#include <cstddef>
#include <memory>
#include <string>

#include "storage/abstract_segment.hpp"
#include "storage/base_value_segment.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment/null_value_vector_iterable.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

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
  const auto data_type = segment.data_type();

  resolve_data_type(data_type, [&](const auto data_type_t) {
    using DataType = typename decltype(data_type_t)::type;

    if (const auto* const value_segment = dynamic_cast<const BaseValueSegment*>(&segment)) {
      _scan_value_segment(*value_segment, chunk_id, matches, position_filter);
    } else if (const auto* const dictionary_segment = dynamic_cast<const BaseDictionarySegment*>(&segment)) {
      _scan_dictionary_segment(*dictionary_segment, chunk_id, matches, position_filter);
    } else if (const auto* const run_length_segment = dynamic_cast<const RunLengthSegment<DataType>*>(&segment)) {
      _scan_run_length_segment(*run_length_segment, chunk_id, matches, position_filter);
    } else if (const auto* const lz4_segment = dynamic_cast<const LZ4Segment<DataType>*>(&segment)) {
      _scan_LZ4_segment(*lz4_segment, chunk_id, matches, position_filter);
    }  else if (const auto* const frame_of_reference_segment = dynamic_cast<const FrameOfReferenceSegment<int32_t>*>(&segment)) {
      _scan_frame_of_reference_segment(*frame_of_reference_segment, chunk_id, matches, position_filter);
    } else {
      const auto& chunk_sorted_by = _in_table->get_chunk(chunk_id)->individually_sorted_by();
      if (!chunk_sorted_by.empty()) {
        for (const auto& sorted_by : chunk_sorted_by) {
          if (sorted_by.column == _column_id) {
            _scan_generic_sorted_segment(segment, chunk_id, matches, position_filter, sorted_by.sort_mode);
            ++num_chunks_with_binary_search;
          }
        }
      }
      _scan_generic_segment(segment, chunk_id, matches, position_filter);
    }
  });

  if (const auto* const value_segment = dynamic_cast<const BaseValueSegment*>(&segment)) {
    _scan_value_segment(*value_segment, chunk_id, matches, position_filter);
  } else if (const auto* const dictionary_segment = dynamic_cast<const BaseDictionarySegment*>(&segment)) {
    _scan_dictionary_segment(*dictionary_segment, chunk_id, matches, position_filter);
  } else {
    const auto& chunk_sorted_by = _in_table->get_chunk(chunk_id)->individually_sorted_by();
    if (!chunk_sorted_by.empty()) {
      for (const auto& sorted_by : chunk_sorted_by) {
        if (sorted_by.column == _column_id) {
          _scan_generic_sorted_segment(segment, chunk_id, matches, position_filter, sorted_by.sort_mode);
          ++num_chunks_with_binary_search;
        }
      }
    }
    _scan_generic_segment(segment, chunk_id, matches, position_filter);
  }
}

void ColumnIsNullTableScanImpl::_scan_generic_segment(
    const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const {
  segment_with_iterators_filtered(segment, position_filter, [&](auto iter, [[maybe_unused]] const auto end) {
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

void ColumnIsNullTableScanImpl::_scan_value_segment(const BaseValueSegment& segment, const ChunkID chunk_id,
                                                    RowIDPosList& matches,
                                                    const std::shared_ptr<const AbstractPosList>& position_filter) {
  if (_matches_all(segment)) {
    _add_all(chunk_id, matches, position_filter ? position_filter->size() : segment.size());
    return;
  }

  if (_matches_none(segment)) {
    ++num_chunks_with_early_out;
    return;
  }

  DebugAssert(segment.is_nullable(), "Columns that are not nullable should have been caught by edge case handling.");

  auto iterable = NullValueVectorIterable{segment.null_values()};

  const auto invert = predicate_condition == PredicateCondition::IsNotNull;
  const auto functor = [&](const auto& value) {
    return invert ^ value.is_null();
  };
  iterable.with_iterators(position_filter, [&](auto iter, auto end) {
    _scan_with_iterators<false>(functor, iter, end, chunk_id, matches);
  });
}

void ColumnIsNullTableScanImpl::_scan_dictionary_segment(
    const BaseDictionarySegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) {
  if (_matches_all(segment)) {
    _add_all(chunk_id, matches, position_filter ? position_filter->size() : segment.size());
    return;
  }

  if (_matches_none(segment)) {
    ++num_chunks_with_early_out;
    return;
  }

  auto iterable = create_iterable_from_attribute_vector(segment);

  const auto invert = predicate_condition == PredicateCondition::IsNotNull;
  const auto functor = [&](const auto& value) {
    return invert ^ value.is_null();
  };
  
  iterable.with_iterators(position_filter, [&](auto iter, auto end) {
    _scan_with_iterators<false>(functor, iter, end, chunk_id, matches);
  });
}

template <typename T>
void ColumnIsNullTableScanImpl::_scan_run_length_segment(
    const RunLengthSegment<T>& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) {
  auto iterable = NullValueVectorIterable{*segment.null_values()};

  const auto invert = predicate_condition == PredicateCondition::IsNotNull;
  const auto functor = [&](const auto& value) {
    return invert ^ value.is_null();
  };

  iterable.with_iterators(position_filter, [&](auto iter, auto end) {
    _scan_with_iterators<false>(functor, iter, end, chunk_id, matches);
  });
}

template <typename T>
void ColumnIsNullTableScanImpl::_scan_LZ4_segment(
    const LZ4Segment<T>& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) {
  auto iterable = NullValueVectorIterable{*segment.null_values()};

  const auto invert = predicate_condition == PredicateCondition::IsNotNull;
  const auto functor = [&](const auto& value) {
    return invert ^ value.is_null();
  };

  iterable.with_iterators(position_filter, [&](auto iter, auto end) {
    _scan_with_iterators<false>(functor, iter, end, chunk_id, matches);
  });
}

template <typename T>
void ColumnIsNullTableScanImpl::_scan_frame_of_reference_segment(
    const FrameOfReferenceSegment<T>& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) {
  auto iterable = NullValueVectorIterable{*segment.null_values()};

  const auto invert = predicate_condition == PredicateCondition::IsNotNull;
  const auto functor = [&](const auto& value) {
    return invert ^ value.is_null();
  };

  iterable.with_iterators(position_filter, [&](auto iter, auto end) {
    _scan_with_iterators<false>(functor, iter, end, chunk_id, matches);
  });
}

bool ColumnIsNullTableScanImpl::_matches_all(const BaseValueSegment& segment) const {
  switch (predicate_condition) {
    case PredicateCondition::IsNull:
      return false;

    case PredicateCondition::IsNotNull:
      return !segment.is_nullable();

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool ColumnIsNullTableScanImpl::_matches_none(const BaseValueSegment& segment) const {
  switch (predicate_condition) {
    case PredicateCondition::IsNull:
      return !segment.is_nullable();

    case PredicateCondition::IsNotNull:
      return false;

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool ColumnIsNullTableScanImpl::_matches_all(const BaseDictionarySegment& segment) const {
  switch (predicate_condition) {
    case PredicateCondition::IsNull:
      return segment.unique_values_count() == 0;

    case PredicateCondition::IsNotNull:
      return segment.unique_values_count() == segment.size();

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool ColumnIsNullTableScanImpl::_matches_none(const BaseDictionarySegment& segment) const {
  switch (predicate_condition) {
    case PredicateCondition::IsNull:
      return segment.unique_values_count() == segment.size();

    case PredicateCondition::IsNotNull:
      return segment.unique_values_count() == 0;

    default:
      Fail("Unsupported comparison type encountered");
  }
}

void ColumnIsNullTableScanImpl::_add_all(const ChunkID chunk_id, RowIDPosList& matches, const size_t segment_size) {
  const auto num_rows = segment_size;
  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < num_rows; ++chunk_offset) {
    matches.emplace_back(chunk_id, chunk_offset);
  }
}

}  // namespace hyrise
