#include "bind_server_prepared_statement_task.hpp"

#include "concurrency/transaction_manager.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline.hpp"

namespace opossum {

void BindServerPreparedStatementTask::_on_execute() {
  try {
    const auto placeholder_plan = _sql_pipeline->get_query_plans().front();

    auto query_plan = std::make_unique<SQLQueryPlan>(placeholder_plan->deep_copy());
    query_plan->tree_roots().at(0)->set_parameters(_parameters);

    _promise.set_value(std::move(query_plan));
  } catch (const std::exception& exception) {
    _promise.set_exception(boost::current_exception());
  }
}

}  // namespace opossum
