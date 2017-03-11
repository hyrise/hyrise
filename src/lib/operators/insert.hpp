#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"

namespace opossum {

class TransactionContext;

/**
 * Operator that inserts a number of rows from one table into another.
 * Expects the table name of the table to insert into as a string and
 * the values to insert in a separate table using the same column layout.
 *
 * Assumption: The input has been validated before.
 */
class Insert : public AbstractReadWriteOperator {
 public:
  explicit Insert(const std::string& table_name, const std::shared_ptr<AbstractOperator>& values_to_insert);

  void commit_records(const CommitID cid) override;
  void rollback_records() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute(TransactionContext* context) override;

 protected:
  PosList _inserted_rows;

 private:
  const std::string _table_name;
  std::shared_ptr<Table> _table;
};

// We need these classes to perform the dynamic cast into a templated ValueColumn
class AbstractTypedColumnProcessor {
 public:
  virtual void resize_vector(std::shared_ptr<BaseColumn> column, size_t new_size) = 0;
  virtual void copy_data(std::shared_ptr<BaseColumn> source, size_t source_start_index,
                         std::shared_ptr<BaseColumn> target, size_t target_start_index, size_t length) = 0;
};

template <typename T>
class TypedColumnProcessor : public AbstractTypedColumnProcessor {
 public:
  void resize_vector(std::shared_ptr<BaseColumn> column, size_t new_size) override {
    auto casted_col = std::dynamic_pointer_cast<ValueColumn<T>>(column);
    auto& vect = casted_col->values();

    vect.resize(new_size);
  }

  // this copies
  void copy_data(std::shared_ptr<BaseColumn> source, size_t source_start_index, std::shared_ptr<BaseColumn> target,
                 size_t target_start_index, size_t length) override {
    auto casted_target = std::dynamic_pointer_cast<ValueColumn<T>>(target);
    if (!casted_target) throw std::logic_error("Type mismatch");
    auto& vect = casted_target->values();

    if (auto casted_source = std::dynamic_pointer_cast<ValueColumn<T>>(source)) {
      std::copy_n(casted_source->values().begin() + source_start_index, length, vect.begin() + target_start_index);
      // } else if(auto casted_source = std::dynamic_pointer_cast<ReferenceColumn>(source)){
      // since we have no guarantee that a referenceColumn references only a single other column,
      // this would require us to find out the referenced column's type for each single row.
      // instead, we just use the slow path below.
    } else {
      for (auto i = 0u; i < length; i++) {
        vect[target_start_index + i] = type_cast<T>((*source)[source_start_index + i]);
      }
    }
  }
};
}  // namespace opossum
