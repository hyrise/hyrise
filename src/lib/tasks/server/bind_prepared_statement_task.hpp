#pragma once

#include "abstract_server_task.hpp"

#include "all_parameter_variant.hpp"

namespace opossum {

class AbstractOperator;
class PreparedPlan;

// This task is used to bind the actual variables of a prepared statements and return the corresponding query plan.
class BindPreparedStatementTask : public AbstractTask {
 public:
  BindPreparedStatementTask(const std::shared_ptr<PreparedPlan>& prepared_plan,
                                  std::vector<AllTypeVariant> params)
      : _prepared_plan(prepared_plan), _params(std::move(params)) {}

 std::shared_ptr<AbstractOperator> get_pqp();
 protected:
  void _on_execute() override;

  std::shared_ptr<PreparedPlan> _prepared_plan;
  std::vector<AllTypeVariant> _params;
  std::shared_ptr<AbstractOperator> _pqp;
};

}  // namespace opossum
