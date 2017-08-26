#include "single_column_table_scan_impl.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "table_scan_main_loop.hpp"

#include "storage/iterables/attribute_vector_iterable.hpp"
#include "storage/iterables/constant_value_iterable.hpp"
#include "storage/untyped_dictionary_column.hpp"

#include "utils/binary_operators.hpp"

#include "resolve_column_type.hpp"

namespace opossum {

SingleColumnTableScanImpl::SingleColumnTableScanImpl(std::shared_ptr<const Table> in_table,
                                                     const ColumnID left_column_id, const ScanType &scan_type,
                                                     const AllTypeVariant &right_value)
    : BaseSingleColumnTableScanImpl{in_table, left_column_id, scan_type}, _right_value{right_value} {}

void SingleColumnTableScanImpl::handle_value_column(BaseColumn &base_column,
                                                    std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);

  const auto left_column_type = _in_table->column_type(_left_column_id);

  resolve_type(left_column_type, [&](auto type) {
    using Type = typename decltype(type)::type;

    auto &left_column = static_cast<ValueColumn<Type> &>(base_column);

    auto left_column_iterable = _create_iterable_from_column(left_column, context->_mapped_chunk_offsets.get());
    auto right_value_iterable = ConstantValueIterable<Type>{_right_value};

    left_column_iterable.get_iterators([&](auto left_it, auto left_end) {
      right_value_iterable.get_iterators([&](auto right_it, auto right_end) {
        resolve_operator_type(_scan_type, [&](auto comparator) {
          TableScanMainLoop{context->_chunk_id, context->_matches_out}(comparator, left_it, left_end, right_it);  // NOLINT
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
  auto &left_column = static_cast<const UntypedDictionaryColumn &>(base_column);

  // TODO(mjendruk): Find a good heuristic for when simply scanning column is faster (dictionary size -> attribute
  // vector size)

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
  auto left_iterable = AttributeVectorIterable{attribute_vector, context->_mapped_chunk_offsets.get()};

  if (_right_value_matches_all(left_column, search_value_id)) {
    left_iterable.get_iterators([&](auto left_it, auto left_end) {
      for (; left_it != left_end; ++left_it) {
        const auto left = *left_it;

        if (left.is_null()) continue;
        matches_out.push_back(RowID{context->_chunk_id, left.chunk_offset()});
      }
    });

    return;
  }

  if (_right_value_matches_none(left_column, search_value_id)) {
    return;
  }

  auto right_iterable = ConstantValueIterable<ValueID>{search_value_id};

  left_iterable.get_iterators([&](auto left_it, auto left_end) {
    right_iterable.get_iterators([&](auto right_it, auto right_end) {
      this->_resolve_scan_type([&](auto comparator) {
        TableScanMainLoop{chunk_id, matches_out}(comparator, left_it, left_end, right_it);  // NOLINT
      });
    });
  });
}

ValueID SingleColumnTableScanImpl::_get_search_value_id(const UntypedDictionaryColumn &column) {
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

bool SingleColumnTableScanImpl::_right_value_matches_all(const UntypedDictionaryColumn &column,
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

bool SingleColumnTableScanImpl::_right_value_matches_none(const UntypedDictionaryColumn &column,
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
