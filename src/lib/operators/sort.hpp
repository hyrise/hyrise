#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Operator to sort a table by a single column. This implements a stable sort, i.e., rows that share the same value will
 * maintain their relative order.
 * Multi-column sort is not supported yet. For now, you will have to sort by the secondary criterion, then by the first
 *
 * Note: Sort does not support null values at the moment
 */
class Sort : public AbstractReadOnlyOperator {
 public:
  // The parameter chunk_size sets the chunk size of the output table, which will always be materialized
  Sort(const std::shared_ptr<const AbstractOperator> in, const ColumnID column_id,
       const OrderByMode order_by_mode = OrderByMode::Ascending, const size_t output_chunk_size = 0);

  ColumnID column_id() const;
  OrderByMode order_by_mode() const;

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant> &args) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_cleanup() override;

  // The operator is seperated in three different classes. SortImpl is the common templated implementation of the
  // operator. SortImpl* und SortImplMaterializeOutput are extra classes for the visitor pattern. They fulfill a certain
  // task during the Sort process, as described later on.
  template <typename SortColumnType>
  class SortImpl;
  template <typename SortColumnType>
  class SortImplMaterializeSortColumn;
  template <typename SortColumnType>
  class SortImplMaterializeOutput;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;
  const ColumnID _column_id;
  const OrderByMode _order_by_mode;
  const size_t _output_chunk_size;
};

// we need to use the impl pattern because the scan operator of the sort depends on the type of the column
template <typename SortColumnType>
class Sort::SortImpl : public AbstractReadOnlyOperatorImpl {
 public:
  SortImpl(const std::shared_ptr<const Table> table_in, const ColumnID column_id,
           const OrderByMode order_by_mode = OrderByMode::Ascending, const size_t output_chunk_size = 0)
      : _table_in(table_in),
        _column_id(column_id),
        _order_by_mode(order_by_mode),
        _output_chunk_size(output_chunk_size) {
    // initialize a structure wich can be sorted by std::sort
    _row_id_value_vector = std::make_shared<std::vector<std::pair<RowID, SortColumnType>>>();
  }

  std::shared_ptr<const Table> _on_execute() override {
    // 1. Prepare Sort: Creating rowid-value-Structur
    auto preparation =
        std::make_shared<SortImplMaterializeSortColumn<SortColumnType>>(_table_in, _column_id, _row_id_value_vector);
    preparation->execute();

    // 2. After we got our ValueRowID Map we sort the map by the value of the pair
    if (_order_by_mode == OrderByMode::Ascending) {
      sort_with_operator<std::less<>>();
    } else {
      sort_with_operator<std::greater<>>();
    }

    // 3. Materialization of the result: We take the sorted ValueRowID Vector, create chunks fill them until they are
    // full and create the next one. Each chunk is filled row by row.
    auto materialization = std::make_shared<SortImplMaterializeOutput<SortColumnType>>(_table_in, _row_id_value_vector,
                                                                                       _output_chunk_size);
    return materialization->execute();
  }

  template <typename Comp>
  void sort_with_operator() {
    Comp comp;
    std::stable_sort(_row_id_value_vector->begin(), _row_id_value_vector->end(),
                     [comp](std::pair<RowID, SortColumnType> a, std::pair<RowID, SortColumnType> b) {
                       return comp(a.second, b.second);
                     });
  }

  const std::shared_ptr<const Table> _table_in;

  // column to sort by
  const ColumnID _column_id;
  const OrderByMode _order_by_mode;
  // chunk size of the materialized output
  const size_t _output_chunk_size;

  std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> _row_id_value_vector;
};

}  // namespace opossum
