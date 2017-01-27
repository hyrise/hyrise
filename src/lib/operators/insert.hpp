#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "get_table.hpp"

namespace opossum {

class TransactionContext;

// operator to retrieve a table from the StorageManager by specifying its name
class Insert : public AbstractReadWriteOperator {
 public:
  explicit Insert(std::shared_ptr<GetTable> get_table, std::shared_ptr<AbstractOperator> values_to_insert);

  std::shared_ptr<const Table> on_execute(const TransactionContext* context) override;
  void commit(const uint32_t cid) override;
  static void static_abort(const std::shared_ptr<Table> table, const PosList& pos_list);
  void abort() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;

 protected:
  PosList _inserted_rows;
};

// We need these classes to perform the dynamic cast into a templated ValueColumn
class AbstractTypedColumnProcessor {
 public:
  virtual void resize_vector(std::shared_ptr<BaseColumn> column1, std::shared_ptr<BaseColumn> values_to_insert) = 0;
  virtual void move_data(std::shared_ptr<BaseColumn> column1, std::shared_ptr<BaseColumn> values_to_insert,
                         size_t start_index) = 0;
};

template <typename T>
class TypedColumnProcessor : public AbstractTypedColumnProcessor {
 public:
  void resize_vector(std::shared_ptr<BaseColumn> column1, std::shared_ptr<BaseColumn> values_to_insert) override {
    auto casted_col1 = std::dynamic_pointer_cast<ValueColumn<T>>(column1);
    auto& vect = casted_col1->values();

    auto additional_rows = values_to_insert->size();
    vect.resize(vect.size() + additional_rows);
  }

  void move_data(std::shared_ptr<BaseColumn> column1, std::shared_ptr<BaseColumn> values_to_insert,
                 size_t start_index) override {
    auto casted_col1 = std::dynamic_pointer_cast<ValueColumn<T>>(column1);
    auto& vect = casted_col1->values();

    for (auto i = 0u; i < values_to_insert->size(); i++) {
      // TODO(all): dont use [], be smart (maybe discern between value, dict or reference col)
      vect[start_index + i] = type_cast<T>((*values_to_insert)[i]);
    }
  }
};
}  // namespace opossum
