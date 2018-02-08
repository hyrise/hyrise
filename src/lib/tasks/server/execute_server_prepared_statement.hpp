#pragma once

#include "server_task.hpp"

namespace opossum {

class ExecuteServerPreparedStatement : public ServerTask {
 public:
  ExecuteServerPreparedStatement(std::shared_ptr<HyriseSession> session,
                                 std::unique_ptr<SQLQueryPlan> prepared_plan,
                                 std::shared_ptr<TransactionContext> transaction_context)
    : ServerTask(std::move(session)),
      _prepared_plan(std::move(prepared_plan)),
      _transaction_context(std::move(transaction_context)) {}

 protected:
  void _on_execute() override;

  std::unique_ptr<SQLQueryPlan> _prepared_plan;
  std::shared_ptr<TransactionContext> _transaction_context;
};

}  // namespace opossum
