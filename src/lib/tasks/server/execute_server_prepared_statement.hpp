#pragma once

#include "server_task.hpp"

namespace opossum {

class SQLQueryPlan;
class TransactionContext;

class ExecuteServerPreparedStatement : public ServerTask<void> {
 public:
  ExecuteServerPreparedStatement(std::unique_ptr<SQLQueryPlan> prepared_plan,
                                 std::shared_ptr<TransactionContext> transaction_context)
      : _prepared_plan(std::move(prepared_plan)),
        _transaction_context(std::move(transaction_context)) {}

 protected:
  void _on_execute() override;

  std::unique_ptr<SQLQueryPlan> _prepared_plan;
  std::shared_ptr<TransactionContext> _transaction_context;
};

}  // namespace opossum
