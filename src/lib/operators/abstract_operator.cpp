#include "abstract_operator.hpp"

#include <memory>

#include "concurrency/transaction_context.hpp"

namespace opossum {

AbstractOperator::AbstractOperator(const std::shared_ptr<const AbstractOperator> left,
                                   const std::shared_ptr<const AbstractOperator> right)
    : _input_left(left), _input_right(right) {}

void AbstractOperator::execute() {
  if (_transaction_context) _transaction_context->on_operator_started();
  _output = on_execute(_transaction_context);
  if (_transaction_context) _transaction_context->on_operator_finished();
}

// returns the result of the operator
std::shared_ptr<const Table> AbstractOperator::get_output() const { return _output; }

std::shared_ptr<const Table> AbstractOperator::input_table_left() const { return _input_left->get_output(); }

std::shared_ptr<const Table> AbstractOperator::input_table_right() const { return _input_right->get_output(); }

std::shared_ptr<TransactionContext> AbstractOperator::transaction_context() const { return _transaction_context; }

void AbstractOperator::set_transaction_context(std::shared_ptr<TransactionContext> transaction_context) {
  _transaction_context = transaction_context;
}

}  // namespace opossum
