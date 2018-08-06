#include "single_column_table_scan_impl.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "storage/base_dictionary_column.hpp"
#include "storage/column_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/resolve_encoded_column_type.hpp"

#include "resolve_type.hpp"
#include "type_comparison.hpp"

namespace opossum {

SingleColumnTableScanImpl::SingleColumnTableScanImpl(const std::shared_ptr<const Table>& in_table,
                                                     const ColumnID left_column_id,
                                                     const PredicateCondition& predicate_condition,
                                                     const AllTypeVariant& right_value)
    : BaseSingleColumnTableScanImpl{in_table, left_column_id, predicate_condition}, _right_value{right_value} {}

std::shared_ptr<PosList> SingleColumnTableScanImpl::scan_chunk(ChunkID chunk_id) {
  // early outs for specific NULL semantics
  if (variant_is_null(_right_value)) {
    /**
     * Comparing anything with NULL (without using IS [NOT] NULL) will result in NULL.
     * Therefore, these scans will always return an empty position list.
     * Because OpIsNull/OpIsNotNull are handled separately in IsNullTableScanImpl,
     * we can assume that comparing with NULLs here will always return nothing.
     */
    return std::make_shared<PosList>();
  }

  return BaseSingleColumnTableScanImpl::scan_chunk(chunk_id);
}

void SingleColumnTableScanImpl::handle_column(const BaseValueColumn& base_column,
                                              std::shared_ptr<ColumnVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  const auto left_column_type = _in_table->column_data_type(_left_column_id);

  resolve_data_type(left_column_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;

    auto& left_column = static_cast<const ValueColumn<ColumnDataType>&>(base_column);

    auto left_column_iterable = create_iterable_from_column(left_column);

    left_column_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
      with_comparator(_predicate_condition, [&](auto comparator) {
        _unary_scan_with_value(comparator, left_it, left_end, type_cast<ColumnDataType>(_right_value), chunk_id,
                               matches_out);
      });
    });
  });
}

void SingleColumnTableScanImpl::handle_column(const BaseEncodedColumn& base_column,
                                              std::shared_ptr<ColumnVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  const auto left_column_type = _in_table->column_data_type(_left_column_id);

  resolve_data_type(left_column_type, [&](auto type) {
    using Type = typename decltype(type)::type;

    resolve_encoded_column_type<Type>(base_column, [&](const auto& typed_column) {
      auto left_column_iterable = create_iterable_from_column(typed_column);

      left_column_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
        with_comparator(_predicate_condition, [&](auto comparator) {
          _unary_scan_with_value(comparator, left_it, left_end, type_cast<Type>(_right_value), chunk_id, matches_out);
        });
      });
    });
  });
}

void SingleColumnTableScanImpl::handle_column(const BaseDictionaryColumn& base_column,
                                              std::shared_ptr<ColumnVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto chunk_id = context->_chunk_id;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;

  /**
   * ValueID value_id; // left value id
   * Variant value; // right value
   *
   * A ValueID value_id from the attribute vector is included in the result iff
   *
   * Operator           |  Condition
   * value_id == value  |  dict.value_by_value_id(dict.lower_bound(value)) == value && value_id == dict.lower_bound(value)
   * value_id != value  |  dict.value_by_value_id(dict.lower_bound(value)) != value || value_id != dict.lower_bound(value)
   * value_id <  value  |  value_id < dict.lower_bound(value)
   * value_id <= value  |  value_id < dict.upper_bound(value)
   * value_id >  value  |  value_id >= dict.upper_bound(value)
   * value_id >= value  |  value_id >= dict.lower_bound(value)
   */

  const auto search_value_id = _get_search_value_id(base_column);

  /**
   * Early Outs
   *
   * Operator          | All                                   | None
   * value_id == value | !None && unique_values_count == 1     | search_vid == dict.upper_bound(value)
   * value_id != value | search_vid == dict.upper_bound(value) | !All && unique_values_count == 1
   * value_id <  value | search_vid == INVALID_VALUE_ID        | search_vid == 0
   * value_id <= value | search_vid == INVALID_VALUE_ID        | search_vid == 0
   * value_id >  value | search_vid == 0                       | search_vid == INVALID_VALUE_ID
   * value_id >= value | search_vid == 0                       | search_vid == INVALID_VALUE_ID
   */

  auto left_iterable = create_iterable_from_attribute_vector(base_column);

  if (_right_value_matches_all(base_column, search_value_id)) {
    left_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
      static const auto always_true = [](const auto&) { return true; };
      this->_unary_scan(always_true, left_it, left_end, chunk_id, matches_out);
    });

    return;
  }

  if (_right_value_matches_none(base_column, search_value_id)) {
    return;
  }

  left_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
    this->_with_operator_for_dict_column_scan(_predicate_condition, [&](auto comparator) {
      this->_unary_scan_with_value(comparator, left_it, left_end, search_value_id, chunk_id, matches_out);
    });
  });
}

ValueID SingleColumnTableScanImpl::_get_search_value_id(const BaseDictionaryColumn& column) const {
  switch (_predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
    case PredicateCondition::GreaterThanEquals:
      return column.lower_bound(_right_value);

    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
      return column.upper_bound(_right_value);

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool SingleColumnTableScanImpl::_right_value_matches_all(const BaseDictionaryColumn& column,
                                                         const ValueID search_value_id) const {
  switch (_predicate_condition) {
    case PredicateCondition::Equals:
      return search_value_id != column.upper_bound(_right_value) && column.unique_values_count() == size_t{1u};

    case PredicateCondition::NotEquals:
      return search_value_id == column.upper_bound(_right_value);

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

bool SingleColumnTableScanImpl::_right_value_matches_none(const BaseDictionaryColumn& column,
                                                          const ValueID search_value_id) const {
  switch (_predicate_condition) {
    case PredicateCondition::Equals:
      return search_value_id == column.upper_bound(_right_value);

    case PredicateCondition::NotEquals:
      return search_value_id == column.upper_bound(_right_value) && column.unique_values_count() == size_t{1u};

    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
      return search_value_id == ValueID{0u};

    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      return search_value_id == INVALID_VALUE_ID;

    default:
      Fail("Unsupported comparison type encountered");
      return false;
  }
}

}  // namespace opossum
