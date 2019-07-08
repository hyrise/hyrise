#pragma once

#include "scheduler/abstract_task.hpp"

namespace opossum {

class AbstractOperator;
class TransactionContext;
class Table;

// This task takes a query plan of a prepared statement and executes it.
class ExecutePreparedStatementTask : public AbstractTask {
 public:
  explicit ExecutePreparedStatementTask(std::shared_ptr<AbstractOperator> prepared_plan)
      : _prepared_plan(std::move(prepared_plan)) {}

  std::shared_ptr<const Table> get_result_table();

 protected:
  void _on_execute() override;

  std::shared_ptr<AbstractOperator> _prepared_plan;
  std::shared_ptr<const Table> _result_table;
};

}  // namespace opossum
