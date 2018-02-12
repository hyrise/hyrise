#include "execute_server_prepared_statement.hpp"
#include <concurrency/transaction_manager.hpp>

#include "scheduler/current_scheduler.hpp"

namespace opossum {

void ExecuteServerPreparedStatement::_on_execute() {
  try {
    const auto tasks = _prepared_plan->create_tasks();
    CurrentScheduler::schedule_and_wait_for_tasks(tasks);
    _transaction_context->commit();
    auto result_table = tasks.back()->get_operator()->get_output();
    return _session->prepared_executed(std::move(result_table));
  } catch (const std::exception& exception) {
    _transaction_context->rollback();
    return _session->pipeline_error(exception.what());
  }
}

}  // namespace opossum
