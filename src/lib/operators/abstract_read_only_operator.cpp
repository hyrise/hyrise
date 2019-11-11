#include "abstract_read_only_operator.hpp"

#include <memory>

#include "storage/table.hpp"

namespace opossum {

AbstractReadOnlyOperator::AbstractReadOnlyOperator(const OperatorType type,
                                                   const std::shared_ptr<const AbstractOperator>& left,
                                                   const std::shared_ptr<const AbstractOperator>& right,
                                                   const std::shared_ptr<const AbstractLQPNode>& lqp_node)
    : AbstractOperator(type, left, right, lqp_node) {}

std::shared_ptr<const Table> AbstractReadOnlyOperator::_on_execute(std::shared_ptr<TransactionContext>) {
  return _on_execute();
}

}  // namespace opossum
