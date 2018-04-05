#pragma once

#include "abstract_server_task.hpp"

namespace opossum {

class SQLQueryPlan;
class TransactionContext;
class Table;

// This task takes a query plan of a prepared statement and executes it.
class ExecuteServerPreparedStatementTask : public AbstractServerTask<TableCSPtr> {
 public:
  explicit ExecuteServerPreparedStatementTask(SQLQueryPlanSPtr prepared_plan)
      : _prepared_plan(std::move(prepared_plan)) {}

 protected:
  void _on_execute() override;

  SQLQueryPlanSPtr _prepared_plan;
};

}  // namespace opossum
