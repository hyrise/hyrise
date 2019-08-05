#include "abstract_operator.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "concurrency/transaction_context.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "utils/format_bytes.hpp"
#include "utils/format_duration.hpp"
#include "utils/print_directed_acyclic_graph.hpp"
#include "utils/timer.hpp"
#include "utils/tracing/probes.hpp"

namespace opossum {

AbstractOperator::AbstractOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                                   const std::shared_ptr<const AbstractOperator>& right,
                                   std::unique_ptr<OperatorPerformanceData> performance_data)
    : _type(type), _input_left(left), _input_right(right), _performance_data(std::move(performance_data)) {}

OperatorType AbstractOperator::type() const { return _type; }

void AbstractOperator::execute() {
  DTRACE_PROBE1(HYRISE, OPERATOR_STARTED, name().c_str());
  DebugAssert(!_input_left || _input_left->get_output(), "Left input has not yet been executed");
  DebugAssert(!_input_right || _input_right->get_output(), "Right input has not yet been executed");
  DebugAssert(!_output, "Operator has already been executed");

  Timer performance_timer;

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

  _performance_data->walltime = performance_timer.lap();

  DTRACE_PROBE5(HYRISE, OPERATOR_EXECUTED, name().c_str(), _performance_data->walltime.count(),
                _output ? _output->row_count() : 0, _output ? _output->chunk_count() : 0,
                reinterpret_cast<uintptr_t>(this));
}

std::shared_ptr<const Table> AbstractOperator::get_output() const {
  DebugAssert(
      [&]() {
        if (!_output) return true;
        if (_output->chunk_count() <= ChunkID{1}) return true;
        const auto chunk_count = _output->chunk_count();
        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto chunk = _output->get_chunk(chunk_id);
          if (chunk && chunk->size() < 1) return true;
        }
        return true;
      }(),
      "Empty chunk returned from operator " + description());

  return _output;
}

void AbstractOperator::clear_output() { _output = nullptr; }

const std::string AbstractOperator::description(DescriptionMode description_mode) const { return name(); }

std::shared_ptr<AbstractOperator> AbstractOperator::deep_copy() const {
  std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>> copied_ops;
  return _deep_copy_impl(copied_ops);
}

std::shared_ptr<const Table> AbstractOperator::input_table_left() const { return _input_left->get_output(); }

std::shared_ptr<const Table> AbstractOperator::input_table_right() const { return _input_right->get_output(); }

bool AbstractOperator::transaction_context_is_set() const { return _transaction_context.has_value(); }

std::shared_ptr<TransactionContext> AbstractOperator::transaction_context() const {
  DebugAssert(!transaction_context_is_set() || !_transaction_context->expired(),
              "TransactionContext is expired, but SQL Query Executor should still own it (Operator: " + name() + ")");
  return transaction_context_is_set() ? _transaction_context->lock() : nullptr;
}

void AbstractOperator::set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  _transaction_context = transaction_context;
  _on_set_transaction_context(transaction_context);
}

void AbstractOperator::set_transaction_context_recursively(
    const std::weak_ptr<TransactionContext>& transaction_context) {
  set_transaction_context(transaction_context);

  if (_input_left) mutable_input_left()->set_transaction_context_recursively(transaction_context);
  if (_input_right) mutable_input_right()->set_transaction_context_recursively(transaction_context);
}

std::shared_ptr<AbstractOperator> AbstractOperator::mutable_input_left() const {
  return std::const_pointer_cast<AbstractOperator>(_input_left);
}

std::shared_ptr<AbstractOperator> AbstractOperator::mutable_input_right() const {
  return std::const_pointer_cast<AbstractOperator>(_input_right);
}

const OperatorPerformanceData& AbstractOperator::performance_data() const { return *_performance_data; }

std::shared_ptr<const AbstractOperator> AbstractOperator::input_left() const { return _input_left; }

std::shared_ptr<const AbstractOperator> AbstractOperator::input_right() const { return _input_right; }

void AbstractOperator::set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  _on_set_parameters(parameters);
  if (input_left()) mutable_input_left()->set_parameters(parameters);
  if (input_right()) mutable_input_right()->set_parameters(parameters);
}

void AbstractOperator::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {}

void AbstractOperator::_on_cleanup() {}

std::shared_ptr<AbstractOperator> AbstractOperator::_deep_copy_impl(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  const auto copied_ops_iter = copied_ops.find(this);
  if (copied_ops_iter != copied_ops.end()) return copied_ops_iter->second;

  const auto copied_input_left =
      input_left() ? input_left()->_deep_copy_impl(copied_ops) : std::shared_ptr<AbstractOperator>{};
  const auto copied_input_right =
      input_right() ? input_right()->_deep_copy_impl(copied_ops) : std::shared_ptr<AbstractOperator>{};

  const auto copied_op = _on_deep_copy(copied_input_left, copied_input_right);
  if (_transaction_context) copied_op->set_transaction_context(*_transaction_context);

  copied_ops.emplace(this, copied_op);

  return copied_op;
}

std::ostream& operator<<(std::ostream& stream, const AbstractOperator& abstract_operator) {
  const auto get_children_fn = [](const auto& op) {
    std::vector<std::shared_ptr<const AbstractOperator>> children;
    if (op->input_left()) children.emplace_back(op->input_left());
    if (op->input_right()) children.emplace_back(op->input_right());
    return children;
  };

  const auto node_print_fn = [&](const auto& op, auto& fn_stream) {
    fn_stream << op->description();

    // If the operator was already executed, print some info about data and performance
    const auto output = op->get_output();
    if (output) {
      fn_stream << " (" << output->row_count() << " row(s)/" << output->chunk_count() << " chunk(s)/"
                << output->column_count() << " column(s)/";

      fn_stream << format_bytes(output->estimate_memory_usage());
      fn_stream << "/";
      fn_stream << abstract_operator.performance_data() << ")";
    }
  };

  print_directed_acyclic_graph<const AbstractOperator>(abstract_operator.shared_from_this(), get_children_fn,
                                                       node_print_fn, stream);

  return stream;
}

}  // namespace opossum
