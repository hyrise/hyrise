#include "abstract_operator.hpp"

#include <cstddef>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "concurrency/transaction_context.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "operators/operator_performance_data.hpp"
#include "resolve_type.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/format_bytes.hpp"
#include "utils/map_prunable_subquery_predicates.hpp"
#include "utils/print_utils.hpp"
#include "utils/timer.hpp"

namespace hyrise {

AbstractOperator::AbstractOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                                   const std::shared_ptr<const AbstractOperator>& right,
                                   std::unique_ptr<AbstractOperatorPerformanceData> init_performance_data)
    : performance_data(std::move(init_performance_data)), _type(type), _left_input(left), _right_input(right) {
  // This operator informs all input operators that it wants to consume their output (so it is not deleted until this
  // operator eventually executes). Operators that use expressions that might contain uncorrelated subqueries have to
  // call `_search_and_register_uncorrelated_subqueries` to also register as a consumer of these subqueries (see, e.g.,
  // the constructors of TableScan or Projection).
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
    auto transaction_context = _transaction_context ? _transaction_context->lock() : nullptr;
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

  auto performance_timer = Timer{};

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

  for (const auto& subquery_expression : _uncorrelated_subquery_expressions) {
    subquery_expression->pqp->deregister_consumer();
  }

  if constexpr (HYRISE_DEBUG) {
    // Verify that LQP (if set) and PQP match.
    if (lqp_node) {
      const auto& lqp_expressions = lqp_node->output_expressions();
      if (!_output) {
        Assert(lqp_expressions.empty(), "Operator did not produce a result, but the LQP expects it to");
      } else if (std::dynamic_pointer_cast<const DummyTableNode>(lqp_node)) {
        // DummyTableNodes do not produce expressions that are used in the remainder of the LQP and do not need to be
        // tested.
      } else {
        // Check that LQP expressions and PQP columns match. If they do not, this is a severe bug as the operators might
        // be operating on the wrong column. This should not only be caught here, but also by more detailed tests.
        // We cannot check the name of the column as LQP expressions do not know their alias.
        const auto column_count = _output->column_count();
        Assert(column_count == lqp_expressions.size(),
               std::string{"Mismatching number of output columns for "} + name());
        for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
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

    // Verify that nullability of columns and segments match for ValueSegments. Only ValueSegments have an individual
    // `is_nullable` attribute.
    if (_output && _output->type() == TableType::Data) {
      const auto column_count = _output->column_count();
      const auto chunk_count = _output->chunk_count();
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
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

std::string AbstractOperator::description(DescriptionMode /*description_mode*/) const {
  return name();
}

std::shared_ptr<AbstractOperator> AbstractOperator::deep_copy() const {
  auto copied_ops = std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>{};
  auto copy = deep_copy(copied_ops);

  // GetTable operators can store references to TableScans as prunable subquery predicates (see get_table.hpp for
  // details). We must assign the copies of these TableScans after copying the entire PQP (see
  // map_prunable_subquery_predicates.hpp).
  map_prunable_subquery_predicates(copied_ops);

  return copy;
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

  const auto copied_op = _on_deep_copy(copied_left_input, copied_right_input, copied_ops);
  copied_op->lqp_node = lqp_node;

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
  const auto previous_consumers = _consumer_count--;
  Assert(previous_consumers > 0, "Cannot decrement number of consumers when no consumers are left.");
  if (previous_consumers == 1) {
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
  auto lock = std::lock_guard<std::mutex>{_operator_task_mutex};
  // Return the OperatorTask that owns this operator if it already exists.
  auto operator_task = _operator_task.lock();
  if (operator_task) {
    return operator_task;
  }

  if constexpr (HYRISE_DEBUG) {
    // Check whether _operator_task points to NULL, which means it was never initialized before.
    // Taken from: https://stackoverflow.com/a/45507610/5558040
    using weak_null_pointer = std::weak_ptr<OperatorTask>;
    auto is_uninitialized =
        !_operator_task.owner_before(weak_null_pointer{}) && !weak_null_pointer{}.owner_before(_operator_task);
    Assert(is_uninitialized || executed(), "This operator was owned by an OperatorTask that did not execute.");
  }

  operator_task = std::make_shared<OperatorTask>(shared_from_this());
  _operator_task = operator_task;
  if (executed()) {
    // Skip task to reduce scheduling overhead.
    operator_task->skip_operator_task();
    DebugAssert(operator_task->is_done(), "Expected OperatorTask to be marked as done.");
  }

  return operator_task;
}

std::vector<std::shared_ptr<AbstractOperator>> AbstractOperator::uncorrelated_subqueries() const {
  auto subquery_pqps = std::vector<std::shared_ptr<AbstractOperator>>{};
  subquery_pqps.reserve(_uncorrelated_subquery_expressions.size());

  for (const auto& subquery_expression : _uncorrelated_subquery_expressions) {
    subquery_pqps.emplace_back(subquery_expression->pqp);
  }

  return subquery_pqps;
}

void AbstractOperator::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {}

void AbstractOperator::_on_cleanup() {}

void AbstractOperator::_search_and_register_uncorrelated_subqueries(
    const std::shared_ptr<AbstractExpression>& expression) {
  /**
   * Register this operator as a consumer of all uncorrelated subqueries found in `expression` or any of its input
   * expressions. In contrast, we do not register as a consumer of correlated subqueries, which cannot be reused by
   * design. They are fully owned and managed by the ExpressionEvaluator.
   */
  const auto& pqp_subquery_expressions = find_pqp_subquery_expressions(expression);
  const auto total_subuery_count = _uncorrelated_subquery_expressions.size() + pqp_subquery_expressions.size();
  _uncorrelated_subquery_expressions.reserve(total_subuery_count);

  for (const auto& subquery_expression : pqp_subquery_expressions) {
    if (subquery_expression->is_correlated()) {
      continue;
    }
    /**
     * The results of uncorrelated subqueries might be used in the operator's _on_execute() method. Therefore, we
     * 1. register as a consumer and
     * 2. store pointers to (i) return the uncorrelated subqueries when the OperatorTasks are created and (ii)
     *    deregister after execution.
     */
    subquery_expression->pqp->register_consumer();
    _uncorrelated_subquery_expressions.emplace_back(subquery_expression);
  }
}

std::ostream& operator<<(std::ostream& stream, const AbstractOperator& abstract_operator) {
  const auto get_children_fn = [](const auto& op) {
    auto children = std::vector<std::shared_ptr<const AbstractOperator>>{};
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

void AbstractOperator::_transition_to(const OperatorState new_state) {
  const auto previous_state = _state.exchange(new_state);

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
