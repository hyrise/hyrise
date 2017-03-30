#include "operator_task.hpp"

#include <memory>
#include <utility>

namespace opossum {
OperatorTask::OperatorTask(std::shared_ptr<AbstractOperator> op) : _op(std::move(op)) {}

const std::shared_ptr<AbstractOperator> &OperatorTask::get_operator() const { return _op; }

void OperatorTask::on_execute() { _op->execute(); }
}  // namespace opossum
