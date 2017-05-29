#pragma once

#include <memory>
#include <string>

#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_result_operator.hpp"

namespace opossum {

// The SQLQueryOperator takes a SQL query, parses and transforms it.
// The it schedules the resulting execution plan. To get the result
// of the execution plan, it exposes an SQLResultOperator task, which
// will upon completion contain the result table of the query.
class SQLQueryOperator : public AbstractOperator {
 public:
  explicit SQLQueryOperator(const std::string& query);

  const std::string name() const override;

  uint8_t num_in_tables() const override;

  uint8_t num_out_tables() const override;

  const std::shared_ptr<OperatorTask>& get_result_task() const;

 protected:
  std::shared_ptr<const Table> on_execute(std::shared_ptr<TransactionContext> context) override;

  // Raw SQL query string.
  const std::string _query;

  // Result operator, which will be dependent on the full execution of the exec plan.
  std::shared_ptr<SQLResultOperator> _result_op;

  std::shared_ptr<OperatorTask> _result_task;
};

}  // namespace opossum
