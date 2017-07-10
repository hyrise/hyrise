#include "helper.hpp"

#include <memory>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "operators/abstract_operator.hpp"
#include "scheduler/abstract_scheduler.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/operator_task.hpp"

namespace tpcc {

void execute_tasks_with_context(std::vector<std::shared_ptr<opossum::OperatorTask>>& tasks,
                                std::shared_ptr<opossum::TransactionContext> t_context) {
  for (auto& task : tasks) {
    task->get_operator()->set_transaction_context(t_context);
  }
  opossum::CurrentScheduler::schedule_and_wait_for_tasks(tasks);
}

}  // namespace tpcc
