#include "bind_server_prepared_statement_task.hpp"

#include "concurrency/transaction_manager.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline.hpp"

namespace opossum {

void BindServerPreparedStatementTask::_on_execute() {
  try {
    const auto placeholder_plan = _sql_pipeline->get_query_plans().front();
    auto query_plan = std::make_unique<SQLQueryPlan>(placeholder_plan->deep_copy());

    std::unordered_map<ParameterID, AllTypeVariant> pqp_parameters;
    for (auto value_placeholder_id = ValuePlaceholderID{0}; value_placeholder_id < _params.size();
         ++value_placeholder_id) {
      const auto parameter_id = placeholder_plan->parameter_ids().at(value_placeholder_id);
      pqp_parameters.emplace(parameter_id, _params[value_placeholder_id]);
    }
    Assert(query_plan->tree_roots().size(), "Expected just one PQP");
    query_plan->tree_roots().at(0)->set_parameters(pqp_parameters);

    _promise.set_value(std::move(query_plan));
  } catch (const std::exception& exception) {
    _promise.set_exception(boost::current_exception());
  }
}

}  // namespace opossum
