#include "abstract_operator.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "logical_query_plan/abstract_non_query_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "resolve_type.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "utils/assert.hpp"
#include "utils/format_bytes.hpp"
#include "utils/format_duration.hpp"
#include "utils/print_utils.hpp"
#include "utils/timer.hpp"

namespace hyrise {

AbstractOperator::AbstractOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                                   const std::shared_ptr<const AbstractOperator>& right,
                                   std::unique_ptr<AbstractOperatorPerformanceData> init_performance_data)
    : performance_data(std::move(init_performance_data)), _type(type), _left_input(left), _right_input(right) {
  // Tell input operators that we want to consume their output
  if (_left_input) {
    mutable_left_input()->register_consumer();
  }

  if (_right_input) {
    mutable_right_input()->register_consumer();
  }
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
    auto aborted = transaction_context ? transaction_context->aborted() : false;
    auto left_has_executed = _left_input ? _left_input->executed() : false;
    auto right_has_executed = _right_input ? _right_input->executed() : false;
    Assert(executed() || aborted || !left_has_executed || !right_has_executed || _consumer_count == 0,
           "Operator did not execute, but at least one input operator has.");
  }
}

OperatorType AbstractOperator::type() const {
  return _type;
}

bool AbstractOperator::executed() const {
  return _state == OperatorState::ExecutedAndAvailable || _state == OperatorState::ExecutedAndCleared;
}

void AbstractOperator::execute() {
  /**
   * If an operator has already executed, we return immediately. Either because
   *    a) the output has already been set, or
   *    b) because there are no more consumers that need the operator's result.
   * For detailed scenarios see: https://github.com/hyrise/hyrise/pull/2254#discussion_r565253226
   */
  if (executed()) {
    return;
  }
  _transition_to(OperatorState::Running);

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

  _transition_to(OperatorState::ExecutedAndAvailable);

  // Tell input operators that we no longer need their output.
  if (_left_input) {
    mutable_left_input()->deregister_consumer();
  }

  if (_right_input) {
    mutable_right_input()->deregister_consumer();
  }

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
  Assert(_state == OperatorState::ExecutedAndAvailable,
         "Trying to get_output of operator which is not in OperatorState::ExecutedAndAvailable.");
  return _output;
}

void AbstractOperator::clear_output() {
  Assert(_consumer_count == 0, "Cannot clear output since there are still consuming operators.");
  if (_never_clear_output) {
    return;
  }

  _transition_to(OperatorState::ExecutedAndCleared);
  _output = nullptr;
}

std::string AbstractOperator::description(DescriptionMode description_mode) const {
  return name();
}

std::shared_ptr<AbstractOperator> AbstractOperator::deep_copy() const {
  std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>> copied_ops;
  return deep_copy(copied_ops);
}

std::shared_ptr<AbstractOperator> AbstractOperator::deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  const auto copied_ops_iter = copied_ops.find(this);
  if (copied_ops_iter != copied_ops.end()) {
    return copied_ops_iter->second;
  }

  const auto copied_left_input =
      left_input() ? left_input()->deep_copy(copied_ops) : std::shared_ptr<AbstractOperator>{};
  const auto copied_right_input =
      right_input() ? right_input()->deep_copy(copied_ops) : std::shared_ptr<AbstractOperator>{};

  auto copied_op = _on_deep_copy(copied_left_input, copied_right_input, copied_ops);

  /**
   * Set the transaction context so that we can execute the copied plan in the current transaction
   * (see, e.g., ExpressionEvaluator::_evaluate_subquery_expression_for_row)
   */
  if (_transaction_context) {
    copied_op->set_transaction_context(*_transaction_context);
  }

  copied_ops.emplace(this, copied_op);

  return copied_op;
}

std::shared_ptr<const Table> AbstractOperator::left_input_table() const {
  return _left_input->get_output();
}

std::shared_ptr<const Table> AbstractOperator::right_input_table() const {
  return _right_input->get_output();
}

size_t AbstractOperator::consumer_count() const {
  return _consumer_count.load();
}

void AbstractOperator::register_consumer() {
  Assert(_state <= OperatorState::ExecutedAndAvailable,
         "Cannot register as a consumer since operator results have already been cleared.");
  ++_consumer_count;
}

void AbstractOperator::deregister_consumer() {
  DebugAssert(_consumer_count > 0, "Number of tracked consumer operators seems to be invalid.");
  // The following section is locked to prevent clear_output() from being called twice. Otherwise, a race condition
  // as follows might occur:
  //  1) T1 decreases _consumer_count, making it equal to one. After this operation, T1 gets suspended.
  //  2) T2 decreases _consumer_count as well, making it equal to zero. It enters the if statement and calls
  //     clear_output() for the first time.
  //  3) T1 wakes up and continues with the if statement. Since _consumer_count equals zero, it also calls
  //     clear_output(), which leads to an illegal state transition ExecutedAndCleared -> ExecutedAndCleared.
  std::lock_guard<std::mutex> lock(_deregister_consumer_mutex);

  _consumer_count--;
  if (_consumer_count == 0) {
    clear_output();
  }
}

void AbstractOperator::never_clear_output() {
  _never_clear_output = true;
}

bool AbstractOperator::transaction_context_is_set() const {
  return _transaction_context.has_value();
}

std::shared_ptr<TransactionContext> AbstractOperator::transaction_context() const {
  DebugAssert(!transaction_context_is_set() || !_transaction_context->expired(),
              "TransactionContext is expired, but SQL Query Executor should still own it (Operator: " + name() + ")");
  return transaction_context_is_set() ? _transaction_context->lock() : nullptr;
}

void AbstractOperator::set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  Assert(_state == OperatorState::Created,
         "Setting the TransactionContext is allowed for OperatorState::Created only.");
  _transaction_context = transaction_context;
  _on_set_transaction_context(transaction_context);
}

void AbstractOperator::set_transaction_context_recursively(
    const std::weak_ptr<TransactionContext>& transaction_context) {
  set_transaction_context(transaction_context);

  if (_left_input) {
    mutable_left_input()->set_transaction_context_recursively(transaction_context);
  }

  if (_right_input) {
    mutable_right_input()->set_transaction_context_recursively(transaction_context);
  }
}

std::shared_ptr<AbstractOperator> AbstractOperator::mutable_left_input() const {
  return std::const_pointer_cast<AbstractOperator>(_left_input);
}

std::shared_ptr<AbstractOperator> AbstractOperator::mutable_right_input() const {
  return std::const_pointer_cast<AbstractOperator>(_right_input);
}

std::shared_ptr<const AbstractOperator> AbstractOperator::left_input() const {
  return _left_input;
}

std::shared_ptr<const AbstractOperator> AbstractOperator::right_input() const {
  return _right_input;
}

void AbstractOperator::set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  Assert(_state == OperatorState::Created, "Setting parameters is allowed for OperatorState::Created only.");
  if (parameters.empty()) {
    return;
  }

  _on_set_parameters(parameters);
  if (left_input()) {
    mutable_left_input()->set_parameters(parameters);
  }
  if (right_input()) {
    mutable_right_input()->set_parameters(parameters);
  }
}

OperatorState AbstractOperator::state() const {
  return _state;
}

std::shared_ptr<OperatorTask> AbstractOperator::get_or_create_operator_task() {
  std::lock_guard<std::mutex> lock(_operator_task_mutex);
  // Return the OperatorTask that owns this operator if it already exists.
  if (!_operator_task.expired()) {
    return _operator_task.lock();
  }

  if constexpr (HYRISE_DEBUG) {
    // Check whether _operator_task points to NULL, which means it was never initialized before.
    // Taken from: https://stackoverflow.com/a/45507610/5558040
    using weak_null_pointer = std::weak_ptr<OperatorTask>;
    auto is_uninitialized =
        !_operator_task.owner_before(weak_null_pointer{}) && !weak_null_pointer{}.owner_before(_operator_task);
    Assert(is_uninitialized || executed(), "This operator was owned by an OperatorTask that did not execute.");
  }

  auto operator_task = std::make_shared<OperatorTask>(shared_from_this());
  _operator_task = std::weak_ptr<OperatorTask>(operator_task);
  if (executed()) {
    // Skip task to reduce scheduling overhead.
    operator_task->skip_operator_task();
    DebugAssert(operator_task->is_done(), "Expected OperatorTask to be marked as done.");
  }

  return operator_task;
}

void AbstractOperator::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {}

void AbstractOperator::_on_cleanup() {}

std::ostream& operator<<(std::ostream& stream, const AbstractOperator& abstract_operator) {
  const auto get_children_fn = [](const auto& op) {
    std::vector<std::shared_ptr<const AbstractOperator>> children;
    if (op->left_input()) {
      children.emplace_back(op->left_input());
    }

    if (op->right_input()) {
      children.emplace_back(op->right_input());
    }
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

void AbstractOperator::_transition_to(OperatorState new_state) {
  OperatorState previous_state = _state.exchange(new_state);

  // Check the validity of the state transition
  switch (new_state) {
    case OperatorState::Running:
      Assert(previous_state == OperatorState::Created, "Illegal state transition to OperatorState::Running");
      break;
    case OperatorState::ExecutedAndAvailable:
      Assert(previous_state == OperatorState::Running,
             "Illegal state transition to OperatorState::ExecutedAndAvailable");
      break;
    case OperatorState::ExecutedAndCleared:
      Assert(previous_state == OperatorState::ExecutedAndAvailable,
             "Illegal state transition to OperatorState::ExecutedAndCleared");
      break;
    default:
      Fail("Unexpected target state in AbstractOperator.");
  }
}

}  // namespace hyrise
