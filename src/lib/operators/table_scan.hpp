#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "storage/reference_column.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class TableScanImpl;

// operator to filter a table by a single attribute
// output is an table with only reference columns
// to filter by multiple criteria, you can chain the operator
class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<AbstractOperator> in, const std::string &filter_column_name, const std::string &op,
            const AllTypeVariant value);
  virtual void execute();
  virtual std::shared_ptr<Table> get_output() const;

 protected:
  virtual const std::string get_name() const;
  virtual uint8_t get_num_in_tables() const;
  virtual uint8_t get_num_out_tables() const;

  const std::unique_ptr<AbstractOperatorImpl> _impl;
};

// we need to use the impl pattern because the scan operator of the sort depends on the type of the column
template <typename T>
class TableScanImpl : public AbstractOperatorImpl {
 public:
  // supported values for op are {"=", "!=", "<", "<=", ">", ">="}
  // creates a new table with reference columns
  TableScanImpl(const std::shared_ptr<AbstractOperator> in, const std::string &filter_column_name,
                const std::string &op, const AllTypeVariant value)
      : _filter_value(type_cast<T>(value)),
        _table(in->get_output()),
        _filter_column_id(_table->get_column_id_by_name(filter_column_name)),
        _op(op),
        _output(new Table),
        _pos_list(new PosList) {
    // create structure for output table
    for (size_t column_id = 0; column_id < _table->col_count(); ++column_id) {
      std::shared_ptr<ReferenceColumn> ref;
      if (auto ref_col = std::dynamic_pointer_cast<ReferenceColumn>(_table->get_chunk(0).get_column(column_id))) {
        ref = std::make_shared<ReferenceColumn>(ref_col->get_referenced_table(), column_id, _pos_list);
      } else {
        ref = std::make_shared<ReferenceColumn>(_table, column_id, _pos_list);
      }
      _output->add_column(_table->get_column_name(column_id), _table->get_column_type(column_id), false);
      _output->get_chunk(0).add_column(ref);
      // TODO(Anyone): do we want to distinguish between chunk tables and "reference tables"?
    }
  }

  template <typename Comp>
  void execute_with_operator() {
    // distinguishes the cases how the filter attribute is stored, i.e., as reference or value column
    Comp comp;
    if (auto ref_col = std::dynamic_pointer_cast<ReferenceColumn>(_table->get_chunk(0).get_column(_filter_column_id))) {
      // case: filter attribute is stored in a reference column
      auto val_table = ref_col->get_referenced_table();
      std::vector<std::vector<T>> values = {};
      for (size_t chunk = 0; chunk < val_table->chunk_count(); chunk++) {
        if (auto val_col =
                std::dynamic_pointer_cast<ValueColumn<T>>(val_table->get_chunk(chunk).get_column(_filter_column_id))) {
          values.emplace_back(val_col->get_values());
        } else {
          throw std::logic_error("Referenced table must only contain value columns");
        }
      }
      if (auto pos_list_in = ref_col->get_pos_list()) {
        for (size_t pos = 0; pos < pos_list_in->size(); pos++) {
          auto chunk_id = get_chunk_id_from_row_id((*pos_list_in)[pos]);
          auto chunk_offset = get_chunk_offset_from_row_id((*pos_list_in)[pos]);
          if (comp(values[chunk_id][chunk_offset], _filter_value)) {
            _pos_list->emplace_back(get_row_id_from_chunk_id_and_chunk_offset(chunk_id, chunk_offset));
          }
        }
      } else {
        // If pos_list_in is a nullptr the reference column contains all values of the referenced column. Thus all
        // chunks
        // and rows must be scaned.
        for (ChunkID chunk_id = 0; chunk_id < val_table->chunk_count(); ++chunk_id) {
          auto &chunk = val_table->get_chunk(chunk_id);
          auto base_column = chunk.get_column(_filter_column_id);
          auto &values = std::dynamic_pointer_cast<ValueColumn<T>>(base_column)->get_values();
          for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
            if (comp(values[chunk_offset], _filter_value)) {
              _pos_list->emplace_back(get_row_id_from_chunk_id_and_chunk_offset(chunk_id, chunk_offset));
            }
          }
        }
      }
    } else {
      // filter attribute is stored in a value column
      for (ChunkID chunk_id = 0; chunk_id < _table->chunk_count(); ++chunk_id) {
        auto &chunk = _table->get_chunk(chunk_id);
        auto base_column = chunk.get_column(_filter_column_id);
        auto &values = std::dynamic_pointer_cast<ValueColumn<T>>(base_column)->get_values();
        for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
          if (comp(values[chunk_offset], _filter_value)) {
            _pos_list->emplace_back(get_row_id_from_chunk_id_and_chunk_offset(chunk_id, chunk_offset));
          }
        }
      }
    }
  }

  void execute() {
    // Definining all possible operators here might appear odd. Chances are, however, that we will not
    // have a similar comparison anywhere else. Index scans, for example, would not use an adaptable binary
    // predicate, but will have to use different methods (lower_range, upper_range, ...) based on the
    // chosen operator. For now, we can save us some dark template magic by using the switch below.
    // DO NOT copy this code, however, without discussing if there is a better way to avoid code duplication.

    if (_op == "=")
      execute_with_operator<std::equal_to<>>();
    else if (_op == "!=")
      execute_with_operator<std::not_equal_to<>>();
    else if (_op == "<")
      execute_with_operator<std::less<>>();
    else if (_op == "<=")
      execute_with_operator<std::less_equal<>>();
    else if (_op == ">")
      execute_with_operator<std::greater<>>();
    else if (_op == ">=")
      execute_with_operator<std::greater_equal<>>();
    else
      throw std::runtime_error(std::string("unknown operator ") + _op);
  }

  virtual std::shared_ptr<Table> get_output() const { return _output; }

  const T _filter_value;
  const std::shared_ptr<Table> _table;

  // column to filter by
  const size_t _filter_column_id;

  // string representation of comparison operator
  const std::string _op;
  std::shared_ptr<Table> _output;
  std::shared_ptr<PosList> _pos_list;
};
}  // namespace opossum
