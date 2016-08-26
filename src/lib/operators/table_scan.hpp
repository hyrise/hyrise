#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "storage/reference_column.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class TableScanImpl;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<AbstractOperator> in, const std::string &filter_column_name,
            /* const std::string &op,*/ const AllTypeVariant value);
  virtual void execute();
  virtual std::shared_ptr<Table> get_output() const;

 protected:
  virtual const std::string get_name() const;
  virtual uint8_t get_num_in_tables() const;
  virtual uint8_t get_num_out_tables() const;

  const std::unique_ptr<AbstractOperatorImpl> _impl;
};

template <typename T>
class TableScanImpl : public AbstractOperatorImpl {
 public:
  TableScanImpl(const std::shared_ptr<AbstractOperator> in, const std::string &filter_column_name,
                /*const std::string &op,*/ const AllTypeVariant value)
      : _filter_value(type_cast<T>(value)),
        _table(in->get_output()),
        _filter_column_id(_table->get_column_id_by_name(filter_column_name)),
        _output(new Table),
        _pos_list(new PosList) {
    for (size_t column_id = 0; column_id < _table->col_count(); ++column_id) {
      auto ref = std::make_shared<ReferenceColumn>(_table, column_id, _pos_list);
      _output->add_column(_table->get_column_name(column_id), _table->get_column_type(column_id), false);
      _output->get_chunk(0).add_column(ref);
      // TODO(Anyone): do we want to distinguish between chunk tables and "reference tables"?
    }
  }

  void execute() {
    for (ChunkID chunk_id = 0; chunk_id < _table->chunk_count(); ++chunk_id) {
      auto &chunk = _table->get_chunk(chunk_id);
      auto base_column = chunk.get_column(_filter_column_id);

      if (auto val_col = std::dynamic_pointer_cast<ValueColumn<T>>(base_column)) {
        // ValueColumn
        const std::vector<T> &values = val_col->get_values();
        for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
          if (values[chunk_offset] == _filter_value) {
            _pos_list->emplace_back(get_row_id_from_chunk_id_and_chunk_offset(chunk_id, chunk_offset));
          }
        }
      } else {
        // ReferenceColumn
        auto ref_col = std::dynamic_pointer_cast<ReferenceColumn>(base_column);
        auto pos_list = ref_col->get_pos_list();

        // TODO(Anyone): improve when chunk can be derived from position
        for (size_t pos_in_poslist = 0; pos_in_poslist < ref_col->size(); ++pos_in_poslist) {
          RowID row_id = (*pos_list)[pos_in_poslist];
          if (type_cast<T>((*ref_col)[row_id]) == _filter_value) {
            _pos_list->emplace_back(row_id);
          }
        }
      }
    }
  }

  virtual std::shared_ptr<Table> get_output() const { return _output; }

  const T _filter_value;
  const std::shared_ptr<Table> _table;
  const size_t _filter_column_id;
  std::shared_ptr<Table> _output;
  std::shared_ptr<PosList> _pos_list;
};
}  // namespace opossum
