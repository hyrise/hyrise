#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_modifying_operator.hpp"
#include "get_table.hpp"

namespace opossum {

class TransactionContext;

// operator to retrieve a table from the StorageManager by specifying its name
class Update : public AbstractModifyingOperator {
 public:
  explicit Insert(std::shared_ptr<AbstractOperator> table_to_update, std::shared_ptr<AbstractOperator> update_values);

  std::shared_ptr<const Table> on_execute(const TransactionContext* context) override;
  void commit(const uint32_t cid) override;
  static void static_abort(const std::shared_ptr<Table> table, const PosList& pos_list);
  void abort() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;

 protected:
  PosList _inserted_rows;
};
}  // namespace opossum
