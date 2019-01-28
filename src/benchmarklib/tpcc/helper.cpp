

#include "operators/abstract_operator.hpp" // NEEDEDINCLUDE
#include "scheduler/current_scheduler.hpp" // NEEDEDINCLUDE
#include "scheduler/operator_task.hpp" // NEEDEDINCLUDE

namespace opossum {

void execute_tasks_with_context(std::vector<std::shared_ptr<OperatorTask>>& tasks,
                                const std::shared_ptr<TransactionContext>& transaction_context) {
  for (auto& task : tasks) {
    task->get_operator()->set_transaction_context(transaction_context);
  }
  CurrentScheduler::schedule_and_wait_for_tasks(tasks);
}

}  // namespace opossum
