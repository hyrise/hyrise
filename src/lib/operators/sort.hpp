#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

// operator to sort a table by a single column
// Multi-column sort is not supported yet. For now, you will have to sort by the secondary criterion, then by the first
class Sort : public AbstractReadOnlyOperator {
 public:
  Sort(const std::shared_ptr<const AbstractOperator> in, const std::string &sort_column_name,
       const bool ascending = true);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute() override;

  template <typename T>
  class SortImpl;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;
  std::string _sort_column_name;
  bool _ascending;
};

// we need to use the impl pattern because the comparator of the sort depends on the type of the column
template <typename T>
class Sort::SortImpl : public AbstractReadOnlyOperatorImpl {
 public:
  // creates a new table with reference columns
  SortImpl(const std::shared_ptr<const AbstractOperator> in, const std::string &sort_column_name,
           const bool ascending = true)
      : _in_op(in),
        _sort_column_name(sort_column_name),
        _ascending(ascending),
        _pos_list(std::make_shared<PosList>()),
        _row_id_value_vector(std::make_shared<std::vector<std::pair<RowID, T>>>()) {}

  std::shared_ptr<const Table> on_execute() override {
    auto output = std::make_shared<Table>();

    auto in_table = _in_op->get_output();
    auto sort_column_id = in_table->column_id_by_name(_sort_column_name);

    // copy the structure of the input table, creating ReferenceColumns where needed
    for (size_t column_id = 0; column_id < in_table->col_count(); ++column_id) {
      std::shared_ptr<ReferenceColumn> ref;
      if (auto reference_col =
              std::dynamic_pointer_cast<ReferenceColumn>(in_table->get_chunk(0).get_column(column_id))) {
        ref = std::make_shared<ReferenceColumn>(reference_col->referenced_table(), column_id, _pos_list);
      } else {
        ref = std::make_shared<ReferenceColumn>(in_table, column_id, _pos_list);
      }
      output->add_column(in_table->column_name(column_id), in_table->column_type(column_id), false);
      output->get_chunk(0).add_column(ref);
      // TODO(Anyone): do we want to distinguish between chunk tables and "reference tables"?
    }

    // We sort by copying all values and their RowIds into _row_id_value_vector which is then sorted by
    // sort_with_operator. Afterwards, we extract the RowIds and write them to the position list shared by all of our
    // ReferenceColumns

    // Step 1: Add all values to _row_id_value_vector

    for (size_t chunk = 0; chunk < in_table->chunk_count(); chunk++) {
      // distinguishes the cases how the sort attribute is stored, i.e., as reference or value column
      if (auto value_column =
              std::dynamic_pointer_cast<ValueColumn<T>>(in_table->get_chunk(chunk).get_column(sort_column_id))) {
        // case: sort attribute is in a value column
        auto &values = value_column->values();
        for (size_t offset = 0; offset < values.size(); offset++) {
          _row_id_value_vector->emplace_back(in_table->calculate_row_id(chunk, offset), values[offset]);
        }
      } else if (auto referenced_column = std::dynamic_pointer_cast<ReferenceColumn>(
                     in_table->get_chunk(chunk).get_column(sort_column_id))) {
        // case: sort attribute is in a reference column.
        auto val_table = referenced_column->referenced_table();
        std::vector<std::vector<T>> reference_values = {};
        for (size_t chunk = 0; chunk < val_table->chunk_count(); chunk++) {
          if (auto val_col =
                  std::dynamic_pointer_cast<ValueColumn<T>>(val_table->get_chunk(chunk).get_column(sort_column_id))) {
            reference_values.emplace_back(val_col->values());
          } else {
            throw std::logic_error("Referenced table must only contain value columns");
          }
        }
        if (referenced_column->pos_list()) {
          auto pos_list_in = referenced_column->pos_list();
          for (size_t pos = 0; pos < pos_list_in->size(); pos++) {
            auto row_id = (*pos_list_in)[pos];
            auto chunk_info = in_table->locate_row(row_id);
            auto chunk_id = chunk_info.first;
            auto chunk_offset = chunk_info.second;
            // TODO(md) use c++ explode
            _row_id_value_vector->emplace_back(row_id, reference_values[chunk_id][chunk_offset]);
          }
        }
      } else {
        throw std::logic_error("Column must either be a value or reference column");
      }
    }

    // Step 2: Do the actual sort

    if (_ascending) {
      sort_with_operator<std::less<>>();
    } else {
      sort_with_operator<std::greater<>>();
    }

    // Step 3: Get the sorted row ids and write them to the position list

    for (size_t row = 0; row < _row_id_value_vector->size(); row++) {
      _pos_list->emplace_back(_row_id_value_vector->at(row).first);
    }

    return output;
  }

  template <typename Comp>
  void sort_with_operator() {
    Comp comp;
    std::stable_sort(_row_id_value_vector->begin(), _row_id_value_vector->end(),
                     [comp](std::pair<RowID, T> a, std::pair<RowID, T> b) { return comp(a.second, b.second); });
  }

  std::shared_ptr<const AbstractOperator> _in_op;

  // column to sort by
  std::string _sort_column_name;
  const bool _ascending;
  std::shared_ptr<PosList> _pos_list;
  std::shared_ptr<std::vector<std::pair<RowID, T>>> _row_id_value_vector;
};
}  // namespace opossum
