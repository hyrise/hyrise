#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "all_parameter_variant.hpp"
#include "types.hpp"

namespace opossum {

class OperatorTask;
class Table;
class TransactionContext;

enum class OperatorType {
  Aggregate,
  Delete,
  Difference,
  ExportBinary,
  ExportCsv,
  GetTable,
  ImportBinary,
  ImportCsv,
  IndexScan,
  Insert,
  JoinHash,
  JoinIndex,
  JoinNestedLoop,
  JoinSortMerge,
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
  CreateView,
  DropView,
  ShowColumns,
  ShowTables,

  Mock  // for Tests that need to Mock operators
};

// AbstractOperator is the abstract super class for all operators.
// All operators have up to two input tables and one output table.
// Their lifecycle has three phases:
// 1. The operator is constructed. Previous operators are not guaranteed to have already executed, so operators must not
// call get_output in their execute method
// 2. The execute method is called from the outside (usually by the scheduler). This is where the heavy lifting is done.
// By now, the input operators have already executed.
// 3. The consumer (usually another operator) calls get_output. This should be very cheap. It is only guaranteed to
// succeed if execute was called before. Otherwise, a nullptr or an empty table could be returned.
//
// Operators shall not be executed twice.
//
// Find more information about operators in our Wiki: https://github.com/hyrise/hyrise/wiki/operator-concept

class AbstractOperator : public std::enable_shared_from_this<AbstractOperator>, private Noncopyable {
 public:
  AbstractOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator> left = nullptr,
                   const std::shared_ptr<const AbstractOperator> right = nullptr);

  virtual ~AbstractOperator() = default;

  OperatorType type() const;

  // Overriding implementations need to call on_operator_started/finished() on the _transaction_context as well
  virtual void execute();

  // returns the result of the operator
  std::shared_ptr<const Table> get_output() const;

  virtual const std::string name() const = 0;
  virtual const std::string description(DescriptionMode description_mode = DescriptionMode::SingleLine) const;

  // This only checks if the operator has/had a transaction context without having to convert the weak_ptr
  bool transaction_context_is_set() const;

  std::shared_ptr<TransactionContext> transaction_context() const;
  void set_transaction_context(std::weak_ptr<TransactionContext> transaction_context);

  // Calls set_transaction_context on itself and both input operators recursively
  void set_transaction_context_recursively(std::weak_ptr<TransactionContext> transaction_context);

  // Returns a new instance of the same operator with the same configuration.
  // The given arguments are used to replace the ValuePlaceholder objects within the new operator, if applicable.
  // Recursively recreates the input operators and passes the argument list along.
  // An operator needs to implement this method in order to be cacheable.
  virtual std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args = {}) const;

  // Get the input operators.
  std::shared_ptr<const AbstractOperator> input_left() const;
  std::shared_ptr<const AbstractOperator> input_right() const;

  // Return input operators.
  // Note: these methods cast away const for the return shared_ptr of AbstractOperator.
  std::shared_ptr<AbstractOperator> mutable_input_left() const;
  std::shared_ptr<AbstractOperator> mutable_input_right() const;

  // Return the output tables of the inputs
  std::shared_ptr<const Table> input_table_left() const;
  std::shared_ptr<const Table> input_table_right() const;

  struct PerformanceData {
    uint64_t walltime_ns = 0;  // time spent in nanoseconds executing this operator
  };
  const AbstractOperator::PerformanceData& performance_data() const;

  void print(std::ostream& stream = std::cout) const;

 protected:
  // abstract method to actually execute the operator
  // execute and get_output are split into two methods to allow for easier
  // asynchronous execution
  virtual std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) = 0;

  // method that allows operator-specific cleanups for temporary data.
  // separate from _on_execute for readability and as a reminder to
  // clean up after execution (if it makes sense)
  virtual void _on_cleanup();

  void _print_impl(std::ostream& out, std::vector<bool>& levels,
                   std::unordered_map<const AbstractOperator*, size_t>& id_by_operator, size_t& id_counter) const;

  // Looks itself up in @param recreated_ops to support diamond shapes in PQPs, if not found calls _on_recreate()
  std::shared_ptr<AbstractOperator> _recreate_impl(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& recreated_ops,
      const std::vector<AllParameterVariant>& args) const;

  virtual std::shared_ptr<AbstractOperator> _on_recreate(
      const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
      const std::shared_ptr<AbstractOperator>& recreated_input_right) const = 0;

  const OperatorType _type;

  // Shared pointers to input operators, can be nullptr.
  std::shared_ptr<const AbstractOperator> _input_left;
  std::shared_ptr<const AbstractOperator> _input_right;

  // Is nullptr until the operator is executed
  std::shared_ptr<const Table> _output;

  // Weak pointer breaks cyclical dependency between operators and context
  std::optional<std::weak_ptr<TransactionContext>> _transaction_context;

  PerformanceData _performance_data;

  std::weak_ptr<OperatorTask> _operator_task;
};

}  // namespace opossum
