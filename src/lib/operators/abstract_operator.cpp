#include "abstract_operator.hpp"

#include <chrono>
#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"
#include "utils/assert.hpp"

namespace opossum {

AbstractOperator::AbstractOperator(const std::shared_ptr<const AbstractOperator> left,
                                   const std::shared_ptr<const AbstractOperator> right)
    : _input_left(left), _input_right(right) {}

void AbstractOperator::execute() {
  auto start = std::chrono::high_resolution_clock::now();

  auto transaction_context = _transaction_context.lock();

  if (transaction_context) transaction_context->on_operator_started();
  _output = _on_execute(transaction_context);
  if (transaction_context) transaction_context->on_operator_finished();

  // release any temporary data if possible
  _on_cleanup();

  auto end = std::chrono::high_resolution_clock::now();
  _performance_data.walltime_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

// returns the result of the operator
std::shared_ptr<const Table> AbstractOperator::get_output() const {
  DebugAssert(_output, "Either execute() wasn't called or the Operator doesn't produce an output");
  DebugAssert(
      [&]() {
        if (_output->chunk_count() <= ChunkID{1}) return true;
        for (auto chunk_id = ChunkID{0}; chunk_id < _output->chunk_count(); ++chunk_id) {
          if (_output->get_chunk(chunk_id).size() < 1) return true;
        }
        return true;
      }(),
      "Empty chunk returned from operator " + description());

  return _output;
}

const std::string AbstractOperator::description() const { return name(); }

std::shared_ptr<const Table> AbstractOperator::_input_table_left() const { return _input_left->get_output(); }

std::shared_ptr<const Table> AbstractOperator::_input_table_right() const { return _input_right->get_output(); }

std::shared_ptr<TransactionContext> AbstractOperator::transaction_context() const {
  return _transaction_context.lock();
}

void AbstractOperator::set_transaction_context(std::weak_ptr<TransactionContext> transaction_context) {
  _transaction_context = transaction_context;
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
