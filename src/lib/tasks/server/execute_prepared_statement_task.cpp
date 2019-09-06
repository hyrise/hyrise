#include "execute_prepared_statement_task.hpp"

#include "operators/abstract_operator.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/operator_task.hpp"

namespace opossum {

void ExecutePreparedStatementTask::_on_execute() {
  const auto tasks = OperatorTask::make_tasks_from_operator(_prepared_plan, CleanupTemporaries::Yes);
  CurrentScheduler::schedule_and_wait_for_tasks(tasks);
  _result_table = tasks.back()->get_operator()->get_output();
}

std::shared_ptr<const Table> ExecutePreparedStatementTask::get_result_table() { return _result_table; }
}  // namespace opossum
