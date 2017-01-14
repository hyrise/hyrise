#pragma once

#include <vector>

#include "abstract_non_modifying_operator.hpp"
#include "types.hpp"

namespace opossum {

class Validate : public AbstractNonModifyingOperator {
 public:
  Validate(const std::shared_ptr<AbstractOperator> in);
  std::shared_ptr<const Table> on_execute(const TransactionContext *transactionContext) override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  const std::shared_ptr<const Table> _in_table;
  std::shared_ptr<Table> _output;
};

}  // namespace opossum
