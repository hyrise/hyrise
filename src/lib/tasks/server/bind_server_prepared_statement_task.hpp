#pragma once

#include "abstract_server_task.hpp"

#include "all_parameter_variant.hpp"

namespace opossum {

class AbstractOperator;
class LQPPreparedStatement;

// This task is used to bind the actual variables of a prepared statements and return the corresponding query plan.
class BindServerPreparedStatementTask : public AbstractServerTask<std::shared_ptr<AbstractOperator>> {
 public:
  BindServerPreparedStatementTask(const std::shared_ptr<LQPPreparedStatement>& prepared_statement, std::vector<AllTypeVariant> params)
      : _prepared_statement(prepared_statement), _params(std::move(params)) {}

 protected:
  void _on_execute() override;

  std::shared_ptr<LQPPreparedStatement> _prepared_statement;
  std::vector<AllTypeVariant> _params;
};

}  // namespace opossum
