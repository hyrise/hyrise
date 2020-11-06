#include "abstract_read_only_operator.hpp"

#include <memory>

#include "storage/table.hpp"

namespace opossum {

AbstractReadOnlyOperator::~AbstractReadOnlyOperator() {
  if constexpr (HYRISE_DEBUG) {
    bool left_has_executed = !_left_input ? false : _left_input->performance_data->executed;
    bool right_has_executed = !_right_input ? false : _right_input->performance_data->executed;
    Assert(performance_data->executed || !(left_has_executed && right_has_executed),
           "Operator did not execute which is unexpected.");
  }
}

std::shared_ptr<const Table> AbstractReadOnlyOperator::_on_execute(std::shared_ptr<TransactionContext>) {
  return _on_execute();
}

}  // namespace opossum
