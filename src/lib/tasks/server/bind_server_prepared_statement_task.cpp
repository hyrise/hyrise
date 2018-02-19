#include "bind_server_prepared_statement_task.hpp"

#include "concurrency/transaction_manager.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline.hpp"

namespace opossum {

void BindServerPreparedStatementTask::_on_execute() {
  try {
    const auto placeholder_plan = _sql_pipeline->get_query_plans().front();
    placeholder_plan->set_num_parameters(_params.size());

    auto query_plan = std::make_unique<SQLQueryPlan>(placeholder_plan->recreate(_params));

    _promise.set_value(std::move(query_plan));
  } catch (const std::exception& exception) {
    _promise.set_exception(boost::current_exception());
  }
}

}  // namespace opossum
