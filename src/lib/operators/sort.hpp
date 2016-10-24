#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_operator.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

template <typename T>
class SortImpl;

// operator to sort a table by a single column
// Multi-column sort is not supported yet. For now, you will have to sort by the secondary criterion, then by the first
class Sort : public AbstractOperator {
 public:
  Sort(const std::shared_ptr<AbstractOperator> in, const std::string &sort_column_name, const bool ascending = true);
  virtual void execute();
  virtual std::shared_ptr<Table> get_output() const;

 protected:
  virtual const std::string name() const;
  virtual uint8_t num_in_tables() const;
  virtual uint8_t num_out_tables() const;

  const std::unique_ptr<AbstractOperatorImpl> _impl;
};

// we need to use the impl pattern because the comparator of the sort depends on the type of the column
template <typename T>
class SortImpl : public AbstractOperatorImpl {
 public:
  // creates a new table with reference columns
  SortImpl(const std::shared_ptr<AbstractOperator> in, const std::string &sort_column_name, const bool ascending = true)
      : _in_table(in->get_output()),
        _sort_column_id(_in_table->column_id_by_name(sort_column_name)),
        _ascending(ascending),
        _output(new Table),
        _pos_list(new PosList),
        _row_id_value_vector(new std::vector<std::pair<RowID, T>>()) {
    // copy the structure of the input table, creating ReferenceColumns where needed
    for (size_t column_id = 0; column_id < _in_table->col_count(); ++column_id) {
      std::shared_ptr<ReferenceColumn> ref;
      if (auto reference_col =
              std::dynamic_pointer_cast<ReferenceColumn>(_in_table->get_chunk(0).get_column(column_id))) {
        ref = std::make_shared<ReferenceColumn>(reference_col->referenced_table(), column_id, _pos_list);
      } else {
        ref = std::make_shared<ReferenceColumn>(_in_table, column_id, _pos_list);
      }
      _output->add_column(_in_table->column_name(column_id), _in_table->column_type(column_id), false);
      _output->get_chunk(0).add_column(ref);
      // TODO(Anyone): do we want to distinguish between chunk tables and "reference tables"?
    }
  }

  virtual void execute() {
    // We sort by copying all values and their RowIds into _row_id_value_vector which is then sorted by
    // sort_with_operator. Afterwards, we extract the RowIds and write them to the position list shared by all of our
    // ReferenceColumns

    // Step 1: Add all values to _row_id_value_vector

    for (size_t chunk = 0; chunk < _in_table->chunk_count(); chunk++) {
      // distinguishes the cases how the sort attribute is stored, i.e., as reference or value column
      if (auto value_column =
              std::dynamic_pointer_cast<ValueColumn<T>>(_in_table->get_chunk(chunk).get_column(_sort_column_id))) {
        // case: sort attribute is in a value column
        auto &values = value_column->values();
        for (size_t offset = 0; offset < values.size(); offset++) {
          _row_id_value_vector->emplace_back(row_id_from_chunk_id_and_chunk_offset(chunk, offset), values[offset]);
        }
      } else if (auto referenced_column = std::dynamic_pointer_cast<ReferenceColumn>(
                     _in_table->get_chunk(chunk).get_column(_sort_column_id))) {
        // case: sort attribute is in a reference column.
        auto val_table = referenced_column->referenced_table();
        std::vector<std::vector<T>> reference_values = {};
        for (size_t chunk = 0; chunk < val_table->chunk_count(); chunk++) {
          if (auto val_col =
                  std::dynamic_pointer_cast<ValueColumn<T>>(val_table->get_chunk(chunk).get_column(_sort_column_id))) {
            if (referenced_column->pos_list()) {
              reference_values.emplace_back(val_col->values());
            } else {
              // If referenced_column->pos_list() is a nullptr the reference column contains all values of the
              // referenced column. Thus all rows must be saved.
              auto &values = val_col->values();
              for (size_t offset = 0; offset < values.size(); offset++) {
                _row_id_value_vector->emplace_back(row_id_from_chunk_id_and_chunk_offset(chunk, offset),
                                                   values[offset]);
              }
            }
          } else {
            throw std::logic_error("Referenced table must only contain value columns");
          }
        }
        if (referenced_column->pos_list()) {
          auto pos_list_in = referenced_column->pos_list();
          for (size_t pos = 0; pos < pos_list_in->size(); pos++) {
            auto row_id = (*pos_list_in)[pos];
            auto chunk_id = chunk_id_from_row_id(row_id);
            auto chunk_offset = chunk_offset_from_row_id(row_id);
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
  }

  template <typename Comp>
  void sort_with_operator() {
    Comp comp;
    std::stable_sort(_row_id_value_vector->begin(), _row_id_value_vector->end(),
                     [comp](std::pair<RowID, T> a, std::pair<RowID, T> b) { return comp(a.second, b.second); });
  }

  virtual std::shared_ptr<Table> get_output() const { return _output; }

  const std::shared_ptr<Table> _in_table;

  // column to sort by
  const size_t _sort_column_id;
  const bool _ascending;
  std::shared_ptr<Table> _output;
  std::shared_ptr<PosList> _pos_list;
  std::shared_ptr<std::vector<std::pair<RowID, T>>> _row_id_value_vector;
};
}  // namespace opossum
