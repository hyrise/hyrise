#pragma once

#include "abstract_server_task.hpp"

namespace opossum {

class AbstractOperator;
class TransactionContext;
class Table;

// This task takes a query plan of a prepared statement and executes it.
class ExecuteServerPreparedStatementTask : public AbstractServerTask<std::shared_ptr<const Table>> {
 public:
  explicit ExecuteServerPreparedStatementTask(std::shared_ptr<AbstractOperator> prepared_plan)
      : _prepared_plan(std::move(prepared_plan)) {}

 protected:
  void _on_execute() override;

  std::shared_ptr<AbstractOperator> _prepared_plan;
};

}  // namespace opossum
