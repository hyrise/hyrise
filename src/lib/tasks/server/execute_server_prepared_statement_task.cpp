#include "execute_server_prepared_statement_task.hpp"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

void ExecuteServerPreparedStatementTask::_on_execute() {
  try {
    const auto tasks = _prepared_plan->create_tasks();
    CurrentScheduler::schedule_and_wait_for_tasks(tasks);
    auto result_table = tasks.back()->get_operator()->get_output();
    _promise.set_value(std::move(result_table));
  } catch (const std::exception&) {
    _promise.set_exception(boost::current_exception());
  }
}

}  // namespace opossum
