#include "bind_server_prepared_statement.hpp"
#include <concurrency/transaction_manager.hpp>
#include "scheduler/current_scheduler.hpp"

namespace opossum {

void BindServerPreparedStatement::_on_execute() {
  try {
    const auto placeholder_plan = _sql_pipeline->get_query_plans().front();
    placeholder_plan->set_num_parameters(_params.size());

    auto query_plan = std::make_unique<SQLQueryPlan>(placeholder_plan->recreate(_params));

    auto transaction_context = opossum::TransactionManager::get().new_transaction_context();
    query_plan->set_transaction_context(transaction_context);

    return _session->prepared_bound(std::move(query_plan), std::move(transaction_context));
  } catch (const std::exception& exception) {
    return _session->pipeline_error(exception.what());
  }
}

}  // namespace opossum
