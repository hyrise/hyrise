#include <memory>

#include "begin_transaction_operator.hpp"
#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"

namespace opossum {

BeginTransactionOperator::BeginTransactionOperator() : AbstractReadOnlyOperator(OperatorType::BeginTransaction) {}

const std::string& BeginTransactionOperator::name() const {
  static const auto name = std::string{"BeginTransaction"};
  return name;
}

std::shared_ptr<AbstractOperator> BeginTransactionOperator::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<BeginTransactionOperator>();
}

void BeginTransactionOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> BeginTransactionOperator::_on_execute() { return nullptr; }

}  // namespace opossum
