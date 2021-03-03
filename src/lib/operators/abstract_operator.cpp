#include "abstract_operator.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "concurrency/transaction_context.hpp"
#include "logical_query_plan/abstract_non_query_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "utils/assert.hpp"
#include "utils/format_bytes.hpp"
#include "utils/format_duration.hpp"
#include "utils/print_directed_acyclic_graph.hpp"
#include "utils/timer.hpp"
#include "utils/tracing/probes.hpp"

namespace opossum {

AbstractOperator::AbstractOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                                   const std::shared_ptr<const AbstractOperator>& right,
                                   std::unique_ptr<AbstractOperatorPerformanceData> init_performance_data)
    : performance_data(std::move(init_performance_data)), _type(type), _left_input(left), _right_input(right) {
  // Tell input operators that we want to consume their output
  if (_left_input) mutable_left_input()->register_consumer();
  if (_right_input) mutable_right_input()->register_consumer();
}

AbstractOperator::~AbstractOperator() {
  /**
   * Assert that we used or executed the operator before its disposal.
   *
   * Hack condition to pass some tests:
   *  We assert for _consumer_count == 0 because some tests create operators, but do not execute them. We do not want
   *  to force tests to call execute() on operators when their only purpose is to test, for example, the output of
   *  the description() function.
   */
  if constexpr (HYRISE_DEBUG) {
    auto transaction_context = _transaction_context.has_value() ? _transaction_context->lock() : nullptr;
    bool aborted = transaction_context ? transaction_context->aborted() : false;
    bool left_has_executed = _left_input ? _left_input->executed() : false;
    bool right_has_executed = _right_input ? _right_input->executed() : false;
    Assert(_executed || aborted || !left_has_executed || !right_has_executed || _consumer_count == 0,
           "Operator did not execute, but at least one input operator has.");
  }
}

OperatorType AbstractOperator::type() const { return _type; }

bool AbstractOperator::executed() const {
  DebugAssert(_executed || !_output, "Did not expect to see an output for an unexecuted operator.");
  return _executed;
}

void AbstractOperator::execute() {
  DTRACE_PROBE1(HYRISE, OPERATOR_STARTED, name().c_str());

  /**
   * If an operator has already executed, we return immediately. Either because
   *    a) the output has already been set, or
   *    b) because there are no more consumers that need the operator's result.
   * For detailed scenarios see: https://github.com/hyrise/hyrise/pull/2254#discussion_r565253226
   */
  if (_executed) return;
  Assert(!_output, "Unexpected re-execution of an operator.");

  auto execution_already_started = _execution_started.exchange(true);
  Assert(!execution_already_started, "Operator is already being executed.");

  if constexpr (HYRISE_DEBUG) {
    Assert(!_left_input || _left_input->executed(), "Left input has not yet been executed");
    Assert(!_right_input || _right_input->executed(), "Right input has not yet been executed");
    Assert(!_left_input || _left_input->get_output(), "Left input has no output data.");
    Assert(!_right_input || _right_input->get_output(), "Right input has no output data.");
  }

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

  if (_output) {
    performance_data->has_output = true;
    performance_data->output_row_count = _output->row_count();
    performance_data->output_chunk_count = _output->chunk_count();
  }
  performance_data->walltime = performance_timer.lap();
  _executed = true;

  // Tell input operators that we no longer need their output.
  if (_left_input) mutable_left_input()->deregister_consumer();
  if (_right_input) mutable_right_input()->deregister_consumer();

  DTRACE_PROBE5(HYRISE, OPERATOR_EXECUTED, name().c_str(), performance_data->walltime.count(),
                _output ? _output->row_count() : 0, _output ? _output->chunk_count() : 0,
                reinterpret_cast<uintptr_t>(this));

  if constexpr (HYRISE_DEBUG) {
    // Verify that LQP (if set) and PQP match.
    if (lqp_node) {
      const auto& lqp_expressions = lqp_node->output_expressions();
      if (!_output) {
        Assert(lqp_expressions.empty(), "Operator did not produce a result, but the LQP expects it to");
      } else if (std::dynamic_pointer_cast<const AbstractNonQueryNode>(lqp_node) ||
                 std::dynamic_pointer_cast<const DummyTableNode>(lqp_node)) {
        // AbstractNonQueryNodes do not have any consumable output_expressions, but the corresponding operators return
        // 'OK' for better compatibility with the console and the server. We do not assert anything here.
        // Similarly, DummyTableNodes do not produce expressions that are used in the remainder of the LQP and do not
        // need to be tested.
      } else {
        // Check that LQP expressions and PQP columns match. If they do not, this is a severe bug as the operators might
        // be operating on the wrong column. This should not only be caught here, but also by more detailed tests.
        // We cannot check the name of the column as LQP expressions do not know their alias.
        Assert(_output->column_count() == lqp_expressions.size(),
               std::string{"Mismatching number of output columns for "} + name());
        for (auto column_id = ColumnID{0}; column_id < _output->column_count(); ++column_id) {
          if (_type != OperatorType::Alias) {
            const auto lqp_type = lqp_expressions[column_id]->data_type();
            const auto pqp_type = _output->column_data_type(column_id);
            const auto pqp_name = _output->column_name(column_id);
            Assert(pqp_type == lqp_type,
                   std::string{"Mismatching column type in "} + name() + " for PQP column '" + pqp_name + "'");
          }
        }
      }
    }

    // Verify that nullability of columns and segments match for ValueSegments
    // Only ValueSegments have an individual is_nullable attribute
    if (_output && _output->type() == TableType::Data) {
      for (auto chunk_id = ChunkID{0}; chunk_id < _output->chunk_count(); ++chunk_id) {
        for (auto column_id = ColumnID{0}; column_id < _output->column_count(); ++column_id) {
          const auto& abstract_segment = _output->get_chunk(chunk_id)->get_segment(column_id);
          resolve_data_and_segment_type(*abstract_segment, [&](const auto data_type_t, const auto& segment) {
            using ColumnDataType = typename decltype(data_type_t)::type;
            using SegmentType = std::decay_t<decltype(segment)>;
            if constexpr (std::is_same_v<SegmentType, ValueSegment<ColumnDataType>>) {
              // If segment is nullable, the column must be nullable as well
              Assert(!segment.is_nullable() || _output->column_is_nullable(column_id),
                     std::string{"Nullable segment found in non-nullable column "} + _output->column_name(column_id));
            }
          });
        }
      }
    }
  }
}

std::shared_ptr<const Table> AbstractOperator::get_output() const {
  Assert(_executed, "Trying to get_output of operator that was not executed yet.");
  return _output;
}

void AbstractOperator::clear_output() {
  Assert(_executed, "Unexpected call of clear_output() since operator did not execute yet.");
  Assert(_consumer_count == 0, "Cannot clear output since there are still consuming operators.");
  if (!_never_clear_output) _output = nullptr;
}

std::string AbstractOperator::description(DescriptionMode description_mode) const { return name(); }

std::shared_ptr<AbstractOperator> AbstractOperator::deep_copy() const {
  std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>> copied_ops;
  return deep_copy(copied_ops);
}

std::shared_ptr<AbstractOperator> AbstractOperator::deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  const auto copied_ops_iter = copied_ops.find(this);
  if (copied_ops_iter != copied_ops.end()) return copied_ops_iter->second;

  const auto copied_left_input =
      left_input() ? left_input()->deep_copy(copied_ops) : std::shared_ptr<AbstractOperator>{};
  const auto copied_right_input =
      right_input() ? right_input()->deep_copy(copied_ops) : std::shared_ptr<AbstractOperator>{};

  auto copied_op = _on_deep_copy(copied_left_input, copied_right_input, copied_ops);

  /**
   * Set the transaction context so that we can execute the copied plan in the current transaction
   * (see, e.g., ExpressionEvaluator::_evaluate_subquery_expression_for_row)
   */
  if (_transaction_context) copied_op->set_transaction_context(*_transaction_context);

  copied_ops.emplace(this, copied_op);

  return copied_op;
}

std::shared_ptr<const Table> AbstractOperator::left_input_table() const { return _left_input->get_output(); }

std::shared_ptr<const Table> AbstractOperator::right_input_table() const { return _right_input->get_output(); }

size_t AbstractOperator::consumer_count() const { return _consumer_count.load(); }

void AbstractOperator::register_consumer() { ++_consumer_count; }

void AbstractOperator::deregister_consumer() {
  DebugAssert(_consumer_count > 0, "Number of tracked consumer operators seems to be invalid.");
  _consumer_count--;
  if (_consumer_count == 0) clear_output();
}

void AbstractOperator::never_clear_output() { _never_clear_output = true; }

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

  if (_left_input) mutable_left_input()->set_transaction_context_recursively(transaction_context);
  if (_right_input) mutable_right_input()->set_transaction_context_recursively(transaction_context);
}

std::shared_ptr<AbstractOperator> AbstractOperator::mutable_left_input() const {
  return std::const_pointer_cast<AbstractOperator>(_left_input);
}

std::shared_ptr<AbstractOperator> AbstractOperator::mutable_right_input() const {
  return std::const_pointer_cast<AbstractOperator>(_right_input);
}

std::shared_ptr<const AbstractOperator> AbstractOperator::left_input() const { return _left_input; }

std::shared_ptr<const AbstractOperator> AbstractOperator::right_input() const { return _right_input; }

void AbstractOperator::set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  DebugAssert(!_executed, "Setting parameters on operators that have already executed is illegal.");
  if (parameters.empty()) return;
  _on_set_parameters(parameters);
  if (left_input()) mutable_left_input()->set_parameters(parameters);
  if (right_input()) mutable_right_input()->set_parameters(parameters);
}

void AbstractOperator::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {}

void AbstractOperator::_on_cleanup() {}

std::ostream& operator<<(std::ostream& stream, const AbstractOperator& abstract_operator) {
  const auto get_children_fn = [](const auto& op) {
    std::vector<std::shared_ptr<const AbstractOperator>> children;
    if (op->left_input()) children.emplace_back(op->left_input());
    if (op->right_input()) children.emplace_back(op->right_input());
    return children;
  };

  const auto node_print_fn = [&](const auto& op, auto& fn_stream) {
    fn_stream << op->description();

    // If the operator was already executed, print some info about data and performance
    if (op->executed() && op->get_output()) {
      const auto output = op->get_output();
      fn_stream << " (" << output->row_count() << " row(s)/" << output->chunk_count() << " chunk(s)/"
                << output->column_count() << " column(s)/";

      fn_stream << format_bytes(output->memory_usage(MemoryUsageCalculationMode::Sampled));
      fn_stream << "/" << *abstract_operator.performance_data << ")";
    }
  };

  print_directed_acyclic_graph<const AbstractOperator>(abstract_operator.shared_from_this(), get_children_fn,
                                                       node_print_fn, stream);

  return stream;
}

}  // namespace opossum
