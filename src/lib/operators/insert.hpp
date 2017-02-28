#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"

namespace opossum {

class TransactionContext;

// operator to retrieve a table from the StorageManager by specifying its name
class Insert : public AbstractReadWriteOperator {
 public:
  explicit Insert(std::string table_name, std::shared_ptr<AbstractOperator> values_to_insert);

  std::shared_ptr<const Table> on_execute(TransactionContext* context) override;
  void commit(const uint32_t cid) override;
  static void static_abort(const std::shared_ptr<Table> table, const PosList& pos_list);
  void abort() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;

 protected:
  PosList _inserted_rows;

 private:
  const std::string _table_name;
  std::shared_ptr<Table> _table;
};

// We need these classes to perform the dynamic cast into a templated ValueColumn
class AbstractTypedColumnProcessor {
 public:
  virtual void resize_vector(std::shared_ptr<BaseColumn> column1, size_t new_size) = 0;
  virtual void move_data(std::shared_ptr<BaseColumn> column1, std::shared_ptr<BaseColumn> values_to_insert,
                         size_t start_index, size_t end_index, size_t input_offset) = 0;
};

template <typename T>
class TypedColumnProcessor : public AbstractTypedColumnProcessor {
 public:
  void resize_vector(std::shared_ptr<BaseColumn> column1, size_t new_size) override {
    auto casted_col1 = std::dynamic_pointer_cast<ValueColumn<T>>(column1);
    auto& vect = casted_col1->values();

    vect.resize(new_size);
  }

  void move_data(std::shared_ptr<BaseColumn> column1, std::shared_ptr<BaseColumn> values_to_insert, size_t start_index,
                 size_t end_index, size_t input_offset) override {
    auto num_values_to_insert = end_index - start_index;
    auto casted_col1 = std::dynamic_pointer_cast<ValueColumn<T>>(column1);
    auto& vect = casted_col1->values();

    if (auto column = std::dynamic_pointer_cast<ValueColumn<T>>(values_to_insert)) {
      std::copy_n(column->values().begin() + input_offset, num_values_to_insert, vect.begin() + start_index);
    // } else if(auto ref_col = std::dynamic_pointer_cast<ReferenceColumn>(values_to_insert)){
    // since we have no guarantee that a referenceColumn references only a single other column,
    // this would require us to find out the referenced column's type for each single row.
    // instead, we just use the slow path below.
    } else {
      for (auto i = 0u; i < num_values_to_insert; i++) {
        vect[start_index + i] = type_cast<T>((*values_to_insert)[input_offset + i]);
      }
    }
  }
};
}  // namespace opossum
