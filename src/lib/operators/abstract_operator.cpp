#include "abstract_operator.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "concurrency/transaction_context.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "utils/print_directed_acyclic_graph.hpp"

namespace opossum {

AbstractOperator::AbstractOperator(const std::shared_ptr<const AbstractOperator> left,
                                   const std::shared_ptr<const AbstractOperator> right)
    : _input_left(left), _input_right(right) {}

void AbstractOperator::execute() {
  auto start = std::chrono::high_resolution_clock::now();

  auto transaction_context = this->transaction_context();

  if (transaction_context) {
    /**
     * Do not execute Operators if transaction has been aborted.
     * Not doing so is crucial in order to make sure no other
     * tasks of the Transaction run while the Rollback happens.
     */
    if (transaction_context->aborted()) {
      return;
    }
    transaction_context->on_operator_started();
    _output = _on_execute(transaction_context);
    transaction_context->on_operator_finished();
  } else {
    _output = _on_execute(nullptr);
  }

  // release any temporary data if possible
  _on_cleanup();

  auto end = std::chrono::high_resolution_clock::now();
  _performance_data.walltime_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

// returns the result of the operator
std::shared_ptr<const Table> AbstractOperator::get_output() const {
  DebugAssert(
      [&]() {
        if (_output == nullptr) return true;
        if (_output->chunk_count() <= ChunkID{1}) return true;
        for (auto chunk_id = ChunkID{0}; chunk_id < _output->chunk_count(); ++chunk_id) {
          if (_output->get_chunk(chunk_id)->size() < 1) return true;
        }
        return true;
      }(),
      "Empty chunk returned from operator " + description());

  DebugAssert(!_output || _output->row_count() == 0 || _output->column_count() > 0,
              "Operator " + description() + " did not output any columns");

  return _output;
}

const std::string AbstractOperator::description(DescriptionMode description_mode) const { return name(); }

std::shared_ptr<AbstractOperator> AbstractOperator::recreate(const std::vector<AllParameterVariant>& args) const {
  Fail("Operator " + name() + " does not implement recreation.");
}

std::shared_ptr<const Table> AbstractOperator::_input_table_left() const { return _input_left->get_output(); }

std::shared_ptr<const Table> AbstractOperator::_input_table_right() const { return _input_right->get_output(); }

bool AbstractOperator::transaction_context_is_set() const { return _transaction_context.has_value(); }

std::shared_ptr<TransactionContext> AbstractOperator::transaction_context() const {
  DebugAssert(!transaction_context_is_set() || !_transaction_context->expired(),
              "TransactionContext is expired, but SQL Query Executor should still own it (Operator: " + name() + ")");
  return transaction_context_is_set() ? _transaction_context->lock() : nullptr;
}

void AbstractOperator::set_transaction_context(std::weak_ptr<TransactionContext> transaction_context) {
  _transaction_context = transaction_context;
}

void AbstractOperator::set_transaction_context_recursively(std::weak_ptr<TransactionContext> transaction_context) {
  set_transaction_context(transaction_context);

  if (_input_left != nullptr) mutable_input_left()->set_transaction_context_recursively(transaction_context);
  if (_input_right != nullptr) mutable_input_right()->set_transaction_context_recursively(transaction_context);
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

std::shared_ptr<OperatorTask> AbstractOperator::operator_task() { return _operator_task.lock(); }

void AbstractOperator::set_operator_task(const std::shared_ptr<OperatorTask>& operator_task) {
  DebugAssert(!_operator_task.lock(), "_operator_task was already set");
  _operator_task = operator_task;
}

void AbstractOperator::print(std::ostream& stream) const {
  const auto get_children_fn = [](const auto& op) {
    std::vector<std::shared_ptr<const AbstractOperator>> children;
    if (op->input_left()) children.emplace_back(op->input_left());
    if (op->input_right()) children.emplace_back(op->input_right());
    return children;
  };
  const auto node_print_fn = [](const auto& op, auto& stream) {
    stream << op->description();

    // If the operator was already executed, print some info about data and performance
    const auto output = op->get_output();
    if (output) {
      stream << " (" << output->row_count() << " row(s)/" << output->chunk_count() << " chunk(s)/"
             << output->column_count() << " column(s)/";

      stream << format_bytes(output->estimate_memory_usage());
      stream << "/";
      stream << op->performance_data().walltime_ns << "ns)";
    }
  };

  print_directed_acyclic_graph<const AbstractOperator>(shared_from_this(), get_children_fn, node_print_fn, stream);
}

void AbstractOperator::_on_cleanup() {}

}  // namespace opossum
