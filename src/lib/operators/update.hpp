#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "delete.hpp"
#include "get_table.hpp"
#include "insert.hpp"

namespace opossum {

class TransactionContext;

// operator to retrieve a table from the StorageManager by specifying its name
class Update : public AbstractReadWriteOperator {
 public:
  explicit Update(std::shared_ptr<AbstractOperator> table_to_update, std::shared_ptr<AbstractOperator> update_values);

  std::shared_ptr<const Table> on_execute(const TransactionContext* context) override;
  void commit(const uint32_t cid) override;
  static void static_abort(const std::shared_ptr<Table> table, const PosList& pos_list);
  void abort() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;

 protected:
  std::unique_ptr<Delete> _delete = nullptr;
  std::unique_ptr<Insert> _insert = nullptr;
};
}  // namespace opossum
