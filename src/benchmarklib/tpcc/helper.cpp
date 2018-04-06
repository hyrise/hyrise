#include "helper.hpp"

#include <memory>
#include <vector>

#include "operators/abstract_operator.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/operator_task.hpp"

namespace tpcc {

void execute_tasks_with_context(std::vector<opossum::OperatorTaskSPtr>& tasks,
                                opossum::TransactionContextSPtr t_context) {
  for (auto& task : tasks) {
    task->get_operator()->set_transaction_context(t_context);
  }
  opossum::CurrentScheduler::schedule_and_wait_for_tasks(tasks);
}

}  // namespace tpcc
