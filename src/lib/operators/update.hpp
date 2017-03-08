#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "delete.hpp"
#include "insert.hpp"

namespace opossum {

/**
 * Operator that updates a number of rows from one table with values supplied in another.
 * Expects both tables to have the exact same layout and number of rows.
 * Expects both tables to only have one chunk.
 */
class Update : public AbstractReadWriteOperator {
 public:
  explicit Update(std::shared_ptr<AbstractOperator> table_to_update, std::shared_ptr<AbstractOperator> update_values);

  void commit(const CommitID cid) override;
  void abort() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute(TransactionContext* context) override;
  bool _execution_input_valid(const TransactionContext* context) const;

 protected:
  std::unique_ptr<Delete> _delete;
  std::unique_ptr<Insert> _insert;
};
}  // namespace opossum
