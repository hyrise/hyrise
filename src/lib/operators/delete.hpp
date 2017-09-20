#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"

#include "utils/assert.hpp"

namespace opossum {

/**
 * Operator that deletes a number of rows from one table.
 * Expects a table with one chunk referencing only one table which
 * is passed via the AbstractOperator in the constructor.
 *
 * Assumption: The input has been validated before.
 */
class Delete : public AbstractReadWriteOperator {
 public:
  explicit Delete(const std::string& table_name, const std::shared_ptr<const AbstractOperator>& values_to_delete);

  void commit_records(const CommitID cid) override;
  void rollback_records() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override {
    Fail("Operator " + this->name() + " does not implement recreation.");
    return {};
  }

  void finish_commit() override;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;

 private:
  /**
   * Validates the context and the input table
   */
  bool _execution_input_valid(const std::shared_ptr<TransactionContext>& context) const;

 private:
  const std::string _table_name;
  std::shared_ptr<Table> _table;
  TransactionID _transaction_id;
  std::vector<std::shared_ptr<const PosList>> _pos_lists;
};
}  // namespace opossum
