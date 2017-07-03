#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Validates visibility of records of a table
 * within the context of a given transaction
 *
 * Assumption: Validate happens before joins.
 */
class Validate : public AbstractReadOnlyOperator {
 public:
  explicit Validate(const std::shared_ptr<AbstractOperator> in);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate() const override {
    throw std::runtime_error("Operator " + this->name() + " does not implement recreation.");
  }

 protected:
  std::shared_ptr<const Table> on_execute(std::shared_ptr<TransactionContext> transactionContext) override;
  std::shared_ptr<const Table> on_execute() override;
};

}  // namespace opossum
