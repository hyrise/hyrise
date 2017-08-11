#pragma once

#include <memory>
#include <vector>

namespace opossum {

class OperatorTask;
class TransactionContext;

}  // namespace opossum

namespace tpcc {

void execute_tasks_with_context(std::vector<std::shared_ptr<opossum::OperatorTask>>& tasks,
                                std::shared_ptr<opossum::TransactionContext> t_context);

}  // namespace tpcc
