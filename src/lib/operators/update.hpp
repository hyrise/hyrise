#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "delete.hpp"
#include "insert.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class Update : public AbstractReadWriteOperator {
 public:
  explicit Update(std::shared_ptr<AbstractOperator> table_to_update, std::shared_ptr<AbstractOperator> update_values);

  std::shared_ptr<const Table> on_execute(TransactionContext* context) override;
  void commit(const uint32_t cid) override;
  void abort() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;

 protected:
  bool _execution_input_valid(const TransactionContext* context) const;

 protected:
  std::unique_ptr<Delete> _delete;
  std::unique_ptr<Insert> _insert;
};
}  // namespace opossum
