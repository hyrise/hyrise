#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "all_parameter_variant.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operator_performance_data.hpp"
#include "types.hpp"

namespace opossum {

class OperatorTask;
class Table;
class TransactionContext;

enum class OperatorType {
  Aggregate,
  Alias,
  ChangeMetaTable,
  CreateTable,
  CreatePreparedPlan,
  CreateView,
  DropTable,
  DropView,
  Delete,
  Difference,
  Export,
  GetTable,
  Import,
  IndexScan,
  Insert,
  JoinHash,
  JoinIndex,
  JoinNestedLoop,
  JoinSortMerge,
  JoinVerification,
  Limit,
  Print,
  Product,
  Projection,
  Sort,
  TableScan,
  TableWrapper,
  UnionAll,
  UnionPositions,
  Update,
  Validate,
  Mock  // for Tests that need to Mock operators
};

/**
 * AbstractOperator is the abstract super class for all operators.
 * All operators have up to two input tables and one output table.
 *
 * LIFECYCLE
 *  1. The operator is constructed. Because input operators are not guaranteed to have already executed, operators
 *     must not call get_output in their execute method.
 *  2. The execute method is called from the outside (usually by the scheduler). This is where the heavy lifting is
 *     done. By now, the input operators have already executed.
 *  3. The consumers, usually other operators, call get_output. This should be very cheap.
 *  4. The operator clears its results once the last consumer deregisters.
 *
 * CONSUMER TRACKING
 *  Operators track the number of consuming operators to automate the clearing of operator results. Therefore,
 *  an operator registers as a consumer at all of its input operators. After having executed, an operator deregisters
 *  automatically.
 *
 *     WARNING on handling Subqueries:
 *      This abstract class handles consumer registration/deregistration for input operators only.
 *      Operators that consume subqueries, such as TableScan and Projection, have to register and deregister as
 *      consumers of their subqueries manually. For example:
 *
 *        1. Projection::Projection
 *            - Collect uncorrelated subqueries from each expression's arguments.
 *            - Call register_consumer and store pointers for all uncorrelated subqueries.
 *        2. Projection::_on_execute
 *            - Compute uncorrelated subqueries using ExpressionEvaluator::populate_uncorrelated_subquery_results_cache
 *            - Call deregister_consumer for each uncorrelated subquery.
 *
 *      It is crucial to call register_consumer from the constructor, before the execution starts, to prevent subquery
 *      results from being cleared too early. Otherwise, operators may need to re-execute, which is illegal.
 *
 *      In contrast to uncorrelated subqueries, correlated subqueries are deep-copied for each row that they are
 *      executed on, so the registration happens at execution time in the ExpressionEvaluator.
 *
 * AUTOMATIC CLEARING
 *  Operators clear themselves automatically by calling clear_output when the last consumer deregisters. Note that
 *  top-level operators do not have any consuming operators. Therefore, owning instances, such as
 *  SQLPipelineStatement, have to call clear_output manually or register as consumers themselves.
 *
 *  To disable the automatic clearing in, e.g., tests, one can call never_clear_output.
 *
 * Find more information about operators in our Wiki: https://github.com/hyrise/hyrise/wiki/operator-concept
 */
class AbstractOperator : public std::enable_shared_from_this<AbstractOperator>, private Noncopyable {
 public:
  AbstractOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left = nullptr,
                   const std::shared_ptr<const AbstractOperator>& right = nullptr,
                   std::unique_ptr<AbstractOperatorPerformanceData> performance_data =
                       std::make_unique<OperatorPerformanceData<AbstractOperatorPerformanceData::NoSteps>>());

  virtual ~AbstractOperator();

  OperatorType type() const;

  // Overriding implementations need to call on_operator_started/finished() on the _transaction_context as well
  virtual void execute();

  /**
   * @return true if the operator finished execution, regardless of whether the results have already been cleared.
   */
  bool executed() const;

  /**
   * @returns the result of the operator that has been executed.
   */
  std::shared_ptr<const Table> get_output() const;

  /**
   * Clears the operator's results by releasing the shared pointer to the result table. In case never_clear_output()
   * has been called, nothing will happen.
   */
  void clear_output();

  virtual const std::string& name() const = 0;
  virtual std::string description(DescriptionMode description_mode = DescriptionMode::SingleLine) const;

  // This only checks if the operator has/had a transaction context without having to convert the weak_ptr
  bool transaction_context_is_set() const;

  std::shared_ptr<TransactionContext> transaction_context() const;
  void set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context);

  // Calls set_transaction_context on itself and both input operators recursively
  void set_transaction_context_recursively(const std::weak_ptr<TransactionContext>& transaction_context);

  /**
   * Recursively copies the input operators and
   * @returns a new instance of the same operator with the same configuration. Deduplication of operator plans will be
   * preserved. See lqp_translator.cpp for more info.
   */
  std::shared_ptr<AbstractOperator> deep_copy() const;

  /**
   * Implements AbstractOperator::deep_copy and uses
   * @param copied_ops to preserve deduplication for operator plans. See lqp_translator.cpp for more info.
   */
  std::shared_ptr<AbstractOperator> deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const;

  // Get the input operators.
  std::shared_ptr<const AbstractOperator> left_input() const;
  std::shared_ptr<const AbstractOperator> right_input() const;

  // Return input operators.
  // Note: these methods cast away const for the return shared_ptr of AbstractOperator.
  std::shared_ptr<AbstractOperator> mutable_left_input() const;
  std::shared_ptr<AbstractOperator> mutable_right_input() const;

  // Return the output tables of the inputs
  std::shared_ptr<const Table> left_input_table() const;
  std::shared_ptr<const Table> right_input_table() const;

  // Returns the current count of operators that registered themselves for output consumption
  size_t consumer_count() const;

  // Increases the count of consuming operators by one.
  void register_consumer();

  // Decreases the count of consuming operators by one. If the counter reaches zero, _clear_ouput() is called.
  void deregister_consumer();

  // Disables the automatic and manual clearing of operator results.
  // This function was introduced for several tests that reuse operator results, in e.g. for-loops, and ran into
  // conflicts with auto-cleared operator results.
  void never_clear_output();

  // Set parameters (AllParameterVariants or CorrelatedParameterExpressions) to their respective values
  void set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters);

  // LQP node with which this operator has been created. Might be uninitialized.
  std::shared_ptr<const AbstractLQPNode> lqp_node;

  std::unique_ptr<AbstractOperatorPerformanceData> performance_data;

 protected:
  // abstract method to actually execute the operator
  // execute and get_output are split into two methods to allow for easier
  // asynchronous execution
  virtual std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) = 0;

  // method that allows operator-specific cleanups for temporary data.
  // separate from _on_execute for readability and as a reminder to
  // clean up after execution (if it makes sense)
  virtual void _on_cleanup();

  // override this if the Operator uses Expressions and set the parameters within them
  virtual void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) = 0;

  // override this if the Operator uses Expressions and set the transaction context in the SubqueryExpressions
  virtual void _on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context);

  // An operator needs to implement this function in order to be cacheable.
  virtual std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const = 0;

  const OperatorType _type;

  // Shared pointers to input operators, can be nullptr.
  std::shared_ptr<const AbstractOperator> _left_input;
  std::shared_ptr<const AbstractOperator> _right_input;

  bool _executed = false;
  std::atomic<bool> _execution_started = false;

  // Is nullptr until the operator is executed
  std::shared_ptr<const Table> _output;

  // Weak pointer breaks cyclical dependency between operators and context
  std::optional<std::weak_ptr<TransactionContext>> _transaction_context;

  // We track the number of consuming operators to automate the clearing of operator results.
  std::atomic<int> _consumer_count = 0;

  // Determines whether operator results can be cleared via clear_output().
  bool _never_clear_output = false;
};

std::ostream& operator<<(std::ostream& stream, const AbstractOperator& abstract_operator);

}  // namespace opossum
