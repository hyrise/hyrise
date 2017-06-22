#include "helper.hpp"

#include <memory>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/abstract_scheduler.hpp"

namespace opossum {

void execute_tasks_with_context(std::vector<std::shared_ptr<OperatorTask>> & tasks,
                                std::shared_ptr<TransactionContext> t_context)
{
  for (auto& task : tasks) {
    task->get_operator()->set_transaction_context(t_context);
  }
  AbstractScheduler::schedule_tasks_and_wait(tasks);
}

void print_table(const std::shared_ptr<const Table> table, PrintMode mode, std::ostream& out) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  Print(table_wrapper, out, mode).execute();
}

}  // namespace opossum
