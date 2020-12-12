#include "column_vs_value_table_scan_impl.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "sorted_segment_search.hpp"
#include "storage/base_dictionary_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/segment_iterate.hpp"

#include "resolve_type.hpp"
#include "type_comparison.hpp"

namespace opossum {

ColumnVsValueTableScanImpl::ColumnVsValueTableScanImpl(const std::shared_ptr<const Table>& in_table,
                                                       const ColumnID column_id,
                                                       const PredicateCondition& init_predicate_condition,
                                                       const AllTypeVariant& init_value)
    : AbstractDereferencedColumnTableScanImpl{in_table, column_id, init_predicate_condition},
      value{init_value},
      _column_is_nullable{in_table->column_is_nullable(column_id)} {
  Assert(in_table->column_data_type(column_id) == data_type_from_all_type_variant(value),
         "Cannot use ColumnVsValueTableScanImpl for scan where column and value data type do not match. Use "
         "ExpressionEvaluatorTableScanImpl.");
}

std::string ColumnVsValueTableScanImpl::description() const { return "ColumnVsValue"; }

void ColumnVsValueTableScanImpl::_scan_non_reference_segment(
    const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) {
  const auto& chunk_sorted_by = _in_table->get_chunk(chunk_id)->individually_sorted_by();

  if (!chunk_sorted_by.empty()) {
    for (const auto& sorted_by : chunk_sorted_by) {
      if (sorted_by.column == _column_id) {
        _scan_sorted_segment(segment, chunk_id, matches, position_filter, sorted_by.sort_mode);
        ++num_chunks_with_binary_search;
        return;
      }
    }
  }

  if (const auto* dictionary_segment = dynamic_cast<const BaseDictionarySegment*>(&segment)) {
    _scan_dictionary_segment(*dictionary_segment, chunk_id, matches, position_filter);
  } else {
    _scan_generic_segment(segment, chunk_id, matches, position_filter);
  }
}

void ColumnVsValueTableScanImpl::_scan_generic_segment(
    const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const {
  segment_with_iterators_filtered(segment, position_filter, [&](auto it, [[maybe_unused]] const auto end) {
    // Don't instantiate this for this for DictionarySegments and ReferenceSegments to save compile time.
    // DictionarySegments are handled in _scan_dictionary_segment()
    // ReferenceSegments are handled via position_filter
    if constexpr (!is_dictionary_segment_iterable_v<typename decltype(it)::IterableType> &&
                  !is_reference_segment_iterable_v<typename decltype(it)::IterableType>) {
      using ColumnDataType = typename decltype(it)::ValueType;

      const auto typed_value = boost::get<ColumnDataType>(value);

      with_comparator(predicate_condition, [&](auto predicate_comparator) {
        auto comparator = [predicate_comparator, typed_value](const auto& position) {
          return predicate_comparator(position.value(), typed_value);
        };
        _scan_with_iterators<true>(comparator, it, end, chunk_id, matches);
      });
    } else {
      Fail("Dictionary- and ReferenceSegments have their own code paths and should be handled there");
    }
  });
}

void ColumnVsValueTableScanImpl::_scan_dictionary_segment(
    const BaseDictionarySegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) {
  /**
   * ValueID search_vid;              // left value id
   * AllTypeVariant search_vid_value; // dict.value_by_value_id(search_vid)
   * Variant value;                  // right value
   *
   * A ValueID value_id from the attribute vector is included in the result iff
   *
   * Operator         |  Condition
   * column == value  |  dict.value_by_value_id(dict.lower_bound(value)) == value && value_id == dict.lower_bound(value)
   * column != value  |  dict.value_by_value_id(dict.lower_bound(value)) != value || value_id != dict.lower_bound(value)
   * column <  value  |  value_id < dict.lower_bound(value)
   * column <= value  |  value_id < dict.upper_bound(value)
   * column >  value  |  value_id >= dict.upper_bound(value)
   * column >= value  |  value_id >= dict.lower_bound(value)
   */

  const auto search_value_id = _get_search_value_id(segment);

  /**
   * Early Outs
   *
   * Operator        | All rows match if:                                     | No rows match if:
   * column == value | search_vid_value == value && unique_values_count == 1  | search_vid_value != value
   * column != value | search_vid_value != value                              | search_vid_value == value && unique_values_count == 1
   * column <  value | search_vid == INVALID_VALUE_ID                         | search_vid == 0
   * column <= value | search_vid == INVALID_VALUE_ID                         | search_vid == 0
   * column >  value | search_vid == 0                                        | search_vid == INVALID_VALUE_ID
   * column >= value | search_vid == 0                                        | search_vid == INVALID_VALUE_ID
   */

  auto iterable = create_iterable_from_attribute_vector(segment);

  if (_value_matches_all(segment, search_value_id)) {
    if (_column_is_nullable) {
      // We still have to check for NULLs
      iterable.with_iterators(position_filter, [&](auto it, auto end) {
        static const auto always_true = [](const auto&) { return true; };
        _scan_with_iterators<true>(always_true, it, end, chunk_id, matches);
      });
    } else {
      // No NULLs, all rows match.
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

  if (_value_matches_none(segment, search_value_id)) {
    ++num_chunks_with_early_out;
    return;
  }

  _with_operator_for_dict_segment_scan([&](auto predicate_comparator) {
    auto comparator = [predicate_comparator, search_value_id](const auto& position) {
      return predicate_comparator(position.value(), search_value_id);
    };

    iterable.with_iterators(position_filter, [&](auto it, auto end) {
      // dictionary.size() represents a NULL in the AttributeVector. For some PredicateConditions, we can
      // avoid explicitly checking for it, since the condition (e.g., LessThan) would never return true for
      // dictionary.size() anyway.
      if (predicate_condition == PredicateCondition::Equals ||
          predicate_condition == PredicateCondition::LessThanEquals ||
          predicate_condition == PredicateCondition::LessThan) {
        _scan_with_iterators<false>(comparator, it, end, chunk_id, matches);
      } else {
        _scan_with_iterators<true>(comparator, it, end, chunk_id, matches);
      }
    });
  });
}

void ColumnVsValueTableScanImpl::_scan_sorted_segment(const AbstractSegment& segment, const ChunkID chunk_id,
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
        auto sorted_segment_search = SortedSegmentSearch(segment_begin, segment_end, sort_mode, _column_is_nullable,
                                                         predicate_condition, boost::get<ColumnDataType>(value));

        sorted_segment_search.scan_sorted_segment([&](auto begin, auto end) {
          sorted_segment_search._write_rows_to_matches(begin, end, chunk_id, matches, position_filter);
        });
      });
    }
  });
}

ValueID ColumnVsValueTableScanImpl::_get_search_value_id(const BaseDictionarySegment& segment) const {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
    case PredicateCondition::GreaterThanEquals:
      return segment.lower_bound(value);

    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
      return segment.upper_bound(value);

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool ColumnVsValueTableScanImpl::_value_matches_all(const BaseDictionarySegment& segment,
                                                    const ValueID search_value_id) const {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return search_value_id != INVALID_VALUE_ID && segment.value_of_value_id(search_value_id) == value &&
             segment.unique_values_count() == size_t{1u};

    case PredicateCondition::NotEquals:
      return search_value_id == INVALID_VALUE_ID || segment.value_of_value_id(search_value_id) != value;

    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
      return search_value_id == INVALID_VALUE_ID;

    case PredicateCondition::GreaterThanEquals:
    case PredicateCondition::GreaterThan:
      return search_value_id == ValueID{0u};

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool ColumnVsValueTableScanImpl::_value_matches_none(const BaseDictionarySegment& segment,
                                                     const ValueID search_value_id) const {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return search_value_id == INVALID_VALUE_ID || value != segment.value_of_value_id(search_value_id);

    case PredicateCondition::NotEquals:
      return search_value_id != INVALID_VALUE_ID && value == segment.value_of_value_id(search_value_id) &&
             segment.unique_values_count() == size_t{1u};

    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
      return search_value_id == ValueID{0u};

    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      return search_value_id == INVALID_VALUE_ID;

    default:
      Fail("Unsupported comparison type encountered");
  }
}

}  // namespace opossum
