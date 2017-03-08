#pragma once

#include <memory>
#include <string>

#include "abstract_read_write_operator.hpp"
#include "table_scan.hpp"

namespace opossum {

/**
 * Operator that deletes a number of rows from one table.
 * Expects a table with one chunk referencing only one table which
 * is passed via the AbstractOperator in the constructor.
 */
class Delete : public AbstractReadWriteOperator {
 public:
  explicit Delete(const std::shared_ptr<const AbstractOperator>& op);

  void commit(const CommitID cid) override;
  void abort() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute(TransactionContext* context) override;

 private:
  /**
   * Validates the context and the input table
   */
  bool _execution_input_valid(const TransactionContext* context) const;

 private:
  std::shared_ptr<const PosList> _pos_list;
  std::shared_ptr<Table> _referenced_table;
  TransactionID _transaction_id;
};
}  // namespace opossum
