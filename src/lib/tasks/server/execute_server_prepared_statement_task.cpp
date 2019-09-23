#include "execute_server_prepared_statement_task.hpp"

#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "scheduler/operator_task.hpp"

namespace opossum {

void ExecuteServerPreparedStatementTask::_on_execute() {
  try {
    const auto tasks = OperatorTask::make_tasks_from_operator(_prepared_plan, CleanupTemporaries::Yes);
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
    auto result_table = tasks.back()->get_operator()->get_output();
    _promise.set_value(std::move(result_table));
  } catch (const std::exception&) {
    _promise.set_exception(boost::current_exception());
  }
}

}  // namespace opossum
