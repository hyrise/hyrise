#include "commit_transaction_operator.hpp"
#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"

namespace opossum {

CommitTransactionOperator::CommitTransactionOperator() : AbstractReadOnlyOperator(OperatorType::CommitTransaction) {}

const std::string& CommitTransactionOperator::name() const {
  static const auto name = std::string{"CommitTransaction"};
  return name;
}

std::shared_ptr<AbstractOperator> CommitTransactionOperator::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<CommitTransactionOperator>();
}

void CommitTransactionOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> CommitTransactionOperator::_on_execute() {
  return nullptr;
}

}  // namespace opossum
