#pragma once

#include <memory>
#include <string>

#include "abstract_read_write_operator.hpp"

namespace opossum {

// operator to rollback all operators in the current commit context.
class RollbackRecords : public AbstractReadWriteOperator {
 public:
  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  inline std::shared_ptr<AbstractOperator> recreate() const override {
    throw std::runtime_error("Operator " + this->name() + " does not implement recreation.");
  }

  void commit_records(const CommitID cid) override;
  void rollback_records() override;

 protected:
  /**
   * Calls rollback_records on all read-write operators.
   */
  std::shared_ptr<const Table> on_execute(std::shared_ptr<TransactionContext> context) override;
};
}  // namespace opossum
