#include "abstract_operator.hpp"

#include <chrono>
#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"

namespace opossum {

AbstractOperator::AbstractOperator(const std::shared_ptr<const AbstractOperator> left,
                                   const std::shared_ptr<const AbstractOperator> right)
    : _input_left(left), _input_right(right) {}

void AbstractOperator::execute() {
  auto start = std::chrono::high_resolution_clock::now();

  if (_transaction_context) _transaction_context->on_operator_started();
  _output = _on_execute(_transaction_context);
  if (_transaction_context) _transaction_context->on_operator_finished();

  // release any temporary data if possible
  _on_cleanup();

  auto end = std::chrono::high_resolution_clock::now();
  _performance_data.walltime_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

// returns the result of the operator
std::shared_ptr<const Table> AbstractOperator::get_output() const { return _output; }

const std::string AbstractOperator::description() const { return name(); }

std::shared_ptr<const Table> AbstractOperator::_input_table_left() const { return _input_left->get_output(); }

std::shared_ptr<const Table> AbstractOperator::_input_table_right() const { return _input_right->get_output(); }

std::shared_ptr<TransactionContext> AbstractOperator::transaction_context() const { return _transaction_context; }

void AbstractOperator::set_transaction_context(std::shared_ptr<TransactionContext> transaction_context) {
  _transaction_context = transaction_context;
}

void AbstractOperator::set_transaction_context_recursively(std::shared_ptr<TransactionContext> transaction_context) {
  set_transaction_context(transaction_context);
  
  if (input_left()) mutable_input_left()->set_transaction_context_recursively(transaction_context);
  if (input_right()) mutable_input_right()->set_transaction_context_recursively(transaction_context);
}

std::shared_ptr<AbstractOperator> AbstractOperator::mutable_input_left() const {
  return std::const_pointer_cast<AbstractOperator>(_input_left);
}

std::shared_ptr<AbstractOperator> AbstractOperator::mutable_input_right() const {
  return std::const_pointer_cast<AbstractOperator>(_input_right);
}

const AbstractOperator::PerformanceData& AbstractOperator::performance_data() const { return _performance_data; }

std::shared_ptr<const AbstractOperator> AbstractOperator::input_left() const { return _input_left; }

std::shared_ptr<const AbstractOperator> AbstractOperator::input_right() const { return _input_right; }

void AbstractOperator::_on_cleanup() {}

}  // namespace opossum
