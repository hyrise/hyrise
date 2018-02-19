#pragma once

#include "server_task.hpp"

namespace opossum {

class SQLQueryPlan;
class TransactionContext;
class Table;

class ExecuteServerPreparedStatement : public ServerTask<std::shared_ptr<const Table>> {
 public:
  ExecuteServerPreparedStatement(std::shared_ptr<SQLQueryPlan> prepared_plan,
                                 std::shared_ptr<TransactionContext> transaction_context)
      : _prepared_plan(std::move(prepared_plan)),
        _transaction_context(std::move(transaction_context)) {}

 protected:
  void _on_execute() override;

  std::shared_ptr<SQLQueryPlan> _prepared_plan;
  std::shared_ptr<TransactionContext> _transaction_context;
};

}  // namespace opossum
