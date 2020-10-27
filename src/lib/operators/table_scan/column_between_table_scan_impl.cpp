#include "column_between_table_scan_impl.hpp"

#include <memory>
#include <string>
#include <type_traits>

#include "expression/between_expression.hpp"
#include "sorted_segment_search.hpp"
#include "storage/chunk.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

#include "resolve_type.hpp"
#include "type_comparison.hpp"

namespace opossum {

ColumnBetweenTableScanImpl::ColumnBetweenTableScanImpl(const std::shared_ptr<const Table>& in_table,
                                                       const ColumnID column_id, const AllTypeVariant& init_left_value,
                                                       const AllTypeVariant& init_right_value,
                                                       PredicateCondition init_predicate_condition)
    : AbstractDereferencedColumnTableScanImpl(in_table, column_id, init_predicate_condition),
      left_value{init_left_value},
      right_value{init_right_value},
      _column_is_nullable{in_table->column_is_nullable(column_id)} {
  const auto column_data_type = in_table->column_data_type(column_id);
  Assert(column_data_type == data_type_from_all_type_variant(left_value), "Type of lower bound has to match column");
  Assert(column_data_type == data_type_from_all_type_variant(right_value), "Type of upper bound has to match column");
}

std::string ColumnBetweenTableScanImpl::description() const { return "ColumnBetween"; }

void ColumnBetweenTableScanImpl::_scan_non_reference_segment(
    const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) {
  const auto& chunk_sorted_by = _in_table->get_chunk(chunk_id)->individually_sorted_by();

  // Check if a sorted scan is possible for the current predicate. Do not use the sorted search for predicates on
  // pre-filtered dictionary segments with string data. In this case, the optimized _scan_dictionary_segment() path if
  // faster than the sorted search. Without this workaround, TPC-H Q6 would lose up to 30%. When the iterator issues of
  // #1531 are resolved, the current workaround should be revisited.
  const auto* dictionary_segment = dynamic_cast<const BaseDictionarySegment*>(&segment);
  if (!chunk_sorted_by.empty() &&
      !(dictionary_segment && position_filter && _in_table->column_data_type(_column_id) == DataType::String)) {
    for (const auto& sorted_by : chunk_sorted_by) {
      if (sorted_by.column == _column_id) {
        _scan_sorted_segment(segment, chunk_id, matches, position_filter, sorted_by.sort_mode);
        ++chunk_scans_sorted;
        return;
      }
    }
  }

  // Select optimized or generic scanning implementation based on segment type
  if (dictionary_segment) {
    _scan_dictionary_segment(*dictionary_segment, chunk_id, matches, position_filter);
  } else {
    _scan_generic_segment(segment, chunk_id, matches, position_filter);
  }
}

void ColumnBetweenTableScanImpl::_scan_generic_segment(
    const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const {
  segment_with_iterators_filtered(segment, position_filter, [&](auto it, [[maybe_unused]] const auto end) {
    using ColumnDataType = typename decltype(it)::ValueType;

    // Don't instantiate this for this for DictionarySegments and ReferenceSegments to save compile time.
    // DictionarySegments are handled in _scan_dictionary_segment()
    // ReferenceSegments are handled via position_filter
    if constexpr (!is_dictionary_segment_iterable_v<typename decltype(it)::IterableType> &&
                  !is_reference_segment_iterable_v<typename decltype(it)::IterableType>) {
      const auto typed_left_value = boost::get<ColumnDataType>(left_value);
      const auto typed_right_value = boost::get<ColumnDataType>(right_value);

      with_between_comparator(predicate_condition, [&](auto between_comparator_function) {
        auto between_comparator = [&](const auto& position) {
          return between_comparator_function(position.value(), typed_left_value, typed_right_value);
        };
        _scan_with_iterators<true>(between_comparator, it, end, chunk_id, matches);
      });
    } else {
      Fail("Dictionary and Reference segments have their own code paths and should be handled there");
    }
  });
}

void ColumnBetweenTableScanImpl::_scan_dictionary_segment(
    const BaseDictionarySegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) {
  ValueID lower_bound_value_id;
  if (is_lower_inclusive_between(predicate_condition)) {
    lower_bound_value_id = segment.lower_bound(left_value);
  } else {
    lower_bound_value_id = segment.upper_bound(left_value);
  }

  ValueID upper_bound_value_id;
  if (is_upper_inclusive_between(predicate_condition)) {
    upper_bound_value_id = segment.upper_bound(right_value);
  } else {
    upper_bound_value_id = segment.lower_bound(right_value);
  }

  auto attribute_vector_iterable = create_iterable_from_attribute_vector(segment);

  /**
   * Early out: All entries (possibly except NULLs) match
   */
  // NOLINTNEXTLINE - cpplint is drunk
  if (lower_bound_value_id == ValueID{0} && upper_bound_value_id == INVALID_VALUE_ID) {
    if (_column_is_nullable) {
      // We still have to check for NULLs
      attribute_vector_iterable.with_iterators(position_filter, [&](auto left_it, auto left_end) {
        static const auto always_true = [](const auto&) { return true; };
        _scan_with_iterators<true>(always_true, left_it, left_end, chunk_id, matches);
      });
    } else {
      // No NULLs, all entries match.
      const auto output_size = position_filter ? position_filter->size() : segment.size();
      const auto output_start_offset = matches.size();
      matches.resize(matches.size() + output_size);

      // Make the compiler try harder to vectorize the trivial loop below.
      // This empty block is used to convince clang-format to keep the pragma indented.
      // NOLINTNEXTLINE
      {}  // clang-format off
      #pragma omp simd
      // clang-format on
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(output_size); ++chunk_offset) {
        // `matches` might already contain entries if it is called multiple times by
        // AbstractDereferencedColumnTableScanImpl::_scan_reference_segment.
        matches[output_start_offset + chunk_offset] = RowID{chunk_id, chunk_offset};
      }
    }

    return;
  }

  /**
   * Early out: No entries match
   */
  if (lower_bound_value_id == INVALID_VALUE_ID || lower_bound_value_id >= upper_bound_value_id) {
    ++chunk_scans_skipped;
    return;
  }

  /**
   * No early out possible: Actually scan the attribute vector
   */

  // In order to avoid having to explicitly check for NULL (represented by a ValueID with the value
  // `segment.unique_values_count()`) we may have to adjust the upper bound to not include the NULL-ValueID
  if (upper_bound_value_id == INVALID_VALUE_ID) {
    upper_bound_value_id = segment.unique_values_count();
  }

  const auto value_id_diff = upper_bound_value_id - lower_bound_value_id;
  const auto comparator = [lower_bound_value_id, value_id_diff](const auto& position) {
    // Using < here because the right value id is the upper_bound. Also, because the value ids are integers, we can do
    // a little hack here: (x >= a && x < b) === ((x - a) < (b - a)); cf. https://stackoverflow.com/a/17095534/2204581
    // This is quite a bit faster.
    return (position.value() - lower_bound_value_id) < value_id_diff;
  };

  attribute_vector_iterable.with_iterators(position_filter, [&](auto left_it, auto left_end) {
    // No need to check for NULL because NULL would be represented as a value ID outside of our range
    _scan_with_iterators<false>(comparator, left_it, left_end, chunk_id, matches);
  });
}

void ColumnBetweenTableScanImpl::_scan_sorted_segment(const AbstractSegment& segment, const ChunkID chunk_id,
                                                      RowIDPosList& matches,
                                                      const std::shared_ptr<const AbstractPosList>& position_filter,
                                                      const SortMode sort_mode) const {
  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    using ColumnDataType = typename decltype(type)::type;

    if constexpr (std::is_same_v<std::decay_t<decltype(typed_segment)>, ReferenceSegment>) {
      Fail("Expected ReferenceSegments to be handled before calling this method");
    } else {
      auto segment_iterable = create_iterable_from_segment(typed_segment);
      segment_iterable.with_iterators(position_filter, [&](auto segment_begin, auto segment_end) {
        const auto typed_left_value = boost::get<ColumnDataType>(left_value);
        const auto typed_right_value = boost::get<ColumnDataType>(right_value);
        auto sorted_segment_search = SortedSegmentSearch(segment_begin, segment_end, sort_mode, _column_is_nullable,
                                                         predicate_condition, typed_left_value, typed_right_value);

        sorted_segment_search.scan_sorted_segment([&](auto begin, auto end) {
          sorted_segment_search._write_rows_to_matches(begin, end, chunk_id, matches, position_filter);
        });
      });
    }
  });
}

}  // namespace opossum
