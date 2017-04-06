#pragma once

#include <memory>
#include <string>

#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "scheduler/operator_task.hpp"

namespace opossum {

// The SQLTaskOperator creates this task to be executed after the execution
// of a query plan. This operator only passes through the result of the previous
// operator. This is useful to bind to the result of this operator, before
// the translation of an SQL query has occurred.
class SQLResultOperator : public AbstractReadOnlyOperator {
 public:
  SQLResultOperator();

  const std::string name() const override;

  uint8_t num_in_tables() const override;

  uint8_t num_out_tables() const override;

  std::shared_ptr<const Table> on_execute() override;

  void set_input_operator(const std::shared_ptr<const AbstractOperator> input);

 protected:
  std::shared_ptr<const AbstractOperator> _input;
};

}  // namespace opossum
