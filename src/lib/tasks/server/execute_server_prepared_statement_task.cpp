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
//    _transaction_context->commit();
    auto result_table = tasks.back()->get_operator()->get_output();
    _promise.set_value(std::move(result_table));
  } catch (const std::exception& exception) {
    // TODO: If rolling back the transaction is correct in this case,
    // it should also result in the transaction being reset in the session
    _transaction_context->rollback();
    _promise.set_exception(boost::current_exception());
  }
}

}  // namespace opossum
