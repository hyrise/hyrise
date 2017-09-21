#pragma once

#include <memory>
#include <string>
#include <vector>

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class TransactionContext;

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
// In order to use new operators in server mode, the following steps have to be performed:
//   1. Add a new Operator definition in Protobuf file: `src/lib/network/protos/opossum.proto` and add it to the
//      enumeration in `OperatorVariant` in this file
//   2. The header- and cpp-files for protocol buffer operators will be generated/updated when the opossum lib is built
//   3. Add a method to class OperatorTranslator in `src/lib/network/operator_translator.cpp` and
//      `src/lib/network/operator_translator.hpp` that transforms the protocol buffer objects into the corresponding
//      opossum operator
//   4. Add an entry in the swith-case of OperatorTranslator::translate_proto() to dispatch calls to the method created
//      in step 3
//   5. Write a test in `src/test/network/operator_translator_test.cpp`
//
// Find more information about operators in our Wiki: https://github.com/hyrise/zweirise/wiki/operator-concept

class AbstractOperator : private Noncopyable {
 public:
  AbstractOperator(const std::shared_ptr<const AbstractOperator> left = nullptr,
                   const std::shared_ptr<const AbstractOperator> right = nullptr);

  virtual ~AbstractOperator() = default;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  AbstractOperator(AbstractOperator &&) = default;
  AbstractOperator &operator=(AbstractOperator &&) = default;

  // Overriding implementations need to call on_operator_started/finished() on the _transaction_context as well
  virtual void execute();

  // returns the result of the operator
  std::shared_ptr<const Table> get_output() const;

  virtual const std::string name() const = 0;
  virtual const std::string description() const;

  // returns the number of input tables, range of values is [0, 2]
  virtual uint8_t num_in_tables() const = 0;

  // returns the number of output tables, range of values is [0, 1]
  virtual uint8_t num_out_tables() const = 0;

  std::shared_ptr<TransactionContext> transaction_context() const;
  void set_transaction_context(std::weak_ptr<TransactionContext> transaction_context);

  // Returns a new instance of the same operator with the same configuration.
  // The given arguments are used to replace the ValuePlaceholder objects within the new operator, if applicable.
  // Recursively recreates the input operators and passes the argument list along.
  // An operator needs to implement this method in order to be cacheable.
  virtual std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant> &args) const = 0;

  // Get the input operators.
  std::shared_ptr<const AbstractOperator> input_left() const;
  std::shared_ptr<const AbstractOperator> input_right() const;

  // Return input operators.
  // Note: these methods cast away const for the return shared_ptr of AbstractOperator.
  std::shared_ptr<AbstractOperator> mutable_input_left() const;
  std::shared_ptr<AbstractOperator> mutable_input_right() const;

 protected:
  // abstract method to actually execute the operator
  // execute and get_output are split into two methods to allow for easier
  // asynchronous execution
  virtual std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) = 0;

  // method that allows operator-specific cleanups for temporary data.
  // separate from _on_execute for readability and as a reminder to
  // clean up after execution (if it makes sense)
  virtual void _on_cleanup();

  std::shared_ptr<const Table> _input_table_left() const;
  std::shared_ptr<const Table> _input_table_right() const;

  // Shared pointers to input operators, can be nullptr.
  std::shared_ptr<const AbstractOperator> _input_left;
  std::shared_ptr<const AbstractOperator> _input_right;

  // Is nullptr until the operator is executed
  std::shared_ptr<const Table> _output;

  std::weak_ptr<TransactionContext> _transaction_context;
};

}  // namespace opossum
