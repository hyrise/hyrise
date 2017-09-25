#include "single_column_table_scan_impl.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "storage/base_dictionary_column.hpp"
#include "storage/iterables/attribute_vector_iterable.hpp"
#include "storage/iterables/constant_value_iterable.hpp"
#include "storage/iterables/create_iterable_from_column.hpp"

#include "resolve_type.hpp"

namespace opossum {

SingleColumnTableScanImpl::SingleColumnTableScanImpl(std::shared_ptr<const Table> in_table,
                                                     const ColumnID left_column_id, const ScanType &scan_type,
                                                     const AllTypeVariant &right_value)
    : BaseSingleColumnTableScanImpl{in_table, left_column_id, scan_type}, _right_value{right_value} {}

void SingleColumnTableScanImpl::handle_value_column(BaseColumn &base_column,
                                                    std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto &matches_out = context->_matches_out;
  const auto &mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  const auto left_column_type = _in_table->column_type(_left_column_id);

  resolve_data_type(left_column_type, [&](auto type) {
    using Type = typename decltype(type)::type;

    auto &left_column = static_cast<ValueColumn<Type> &>(base_column);

    auto left_column_iterable = create_iterable_from_column(left_column);
    auto right_value_iterable = ConstantValueIterable<Type>{_right_value};

    left_column_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
      right_value_iterable.with_iterators([&](auto right_it, auto right_end) {
        _with_operator(_scan_type, [&](auto comparator) {
          this->_binary_scan(comparator, left_it, left_end, right_it, chunk_id, matches_out);
        });
      });
    });
  });
}

void SingleColumnTableScanImpl::handle_dictionary_column(BaseColumn &base_column,
                                                         std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto &matches_out = context->_matches_out;
  const auto chunk_id = context->_chunk_id;
  const auto &mapped_chunk_offsets = context->_mapped_chunk_offsets;
  auto &left_column = static_cast<const BaseDictionaryColumn &>(base_column);

  /**
   * ValueID value_id; // left value id
   * Variant value; // right value
   *
   * A ValueID value_id from the attribute vector is included in the result iff
   *
   * Operator           |  Condition
   * value_id == value  |  dict.value_by_value_id(dict.lower_bound(value)) == value && value_id ==
   * dict.lower_bound(value)
   * value_id != value  |  dict.value_by_value_id(dict.lower_bound(value)) != value || value_id !=
   * dict.lower_bound(value)
   * value_id <  value  |  value_id < dict.lower_bound(value)
   * value_id <= value  |  value_id < dict.upper_bound(value)
   * value_id >  value  |  value_id >= dict.upper_bound(value)
   * value_id >= value  |  value_id >= dict.lower_bound(value)
   */

  const auto search_value_id = _get_search_value_id(left_column);

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

  const auto &attribute_vector = *left_column.attribute_vector();
  auto left_iterable = AttributeVectorIterable{attribute_vector};

  if (_right_value_matches_all(left_column, search_value_id)) {
    left_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
      static const auto always_true = [](const auto &) { return true; };
      this->_unary_scan(always_true, left_it, left_end, chunk_id, matches_out);
    });

    return;
  }

  if (_right_value_matches_none(left_column, search_value_id)) {
    return;
  }

  auto right_iterable = ConstantValueIterable<ValueID>{search_value_id};

  left_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
    right_iterable.with_iterators([&](auto right_it, auto right_end) {
      this->_with_operator_for_dict_column_scan(_scan_type, [&](auto comparator) {
        this->_binary_scan(comparator, left_it, left_end, right_it, chunk_id, matches_out);
      });
    });
  });
}

ValueID SingleColumnTableScanImpl::_get_search_value_id(const BaseDictionaryColumn &column) {
  switch (_scan_type) {
    case ScanType::OpEquals:
    case ScanType::OpNotEquals:
    case ScanType::OpLessThan:
    case ScanType::OpGreaterThanEquals:
      return column.lower_bound(_right_value);

    case ScanType::OpLessThanEquals:
    case ScanType::OpGreaterThan:
      return column.upper_bound(_right_value);

    default:
      Fail("Unsupported comparison type encountered");
      return INVALID_VALUE_ID;
  }
}

bool SingleColumnTableScanImpl::_right_value_matches_all(const BaseDictionaryColumn &column,
                                                         const ValueID search_value_id) {
  switch (_scan_type) {
    case ScanType::OpEquals:
      return search_value_id != column.upper_bound(_right_value) && column.unique_values_count() == size_t{1u};

    case ScanType::OpNotEquals:
      return search_value_id == column.upper_bound(_right_value);

    case ScanType::OpLessThan:
    case ScanType::OpLessThanEquals:
      return search_value_id == INVALID_VALUE_ID;

    case ScanType::OpGreaterThanEquals:
    case ScanType::OpGreaterThan:
      return search_value_id == ValueID{0u};

    default:
      Fail("Unsupported comparison type encountered");
      return false;
  }
}

bool SingleColumnTableScanImpl::_right_value_matches_none(const BaseDictionaryColumn &column,
                                                          const ValueID search_value_id) {
  switch (_scan_type) {
    case ScanType::OpEquals:
      return search_value_id == column.upper_bound(_right_value);

    case ScanType::OpNotEquals:
      return search_value_id == column.upper_bound(_right_value) && column.unique_values_count() == size_t{1u};

    case ScanType::OpLessThan:
    case ScanType::OpLessThanEquals:
      return search_value_id == ValueID{0u};

    case ScanType::OpGreaterThan:
    case ScanType::OpGreaterThanEquals:
      return search_value_id == INVALID_VALUE_ID;

    default:
      Fail("Unsupported comparison type encountered");
      return false;
  }
}

}  // namespace opossum
