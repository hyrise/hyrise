#pragma once

#include <memory>
#include <string>
#include <vector>

#include "../common.hpp"
#include "../storage/table.hpp"

namespace opossum {

// AbstractOperator is the abstract super class for all operators.
// All operators have up to two input tables and one output table.
class AbstractOperator {
 public:
  AbstractOperator(const std::shared_ptr<AbstractOperator> left = nullptr,
                   const std::shared_ptr<AbstractOperator> right = nullptr);

  // copying a operator is not allowed
  AbstractOperator(AbstractOperator const&) = delete;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  AbstractOperator(AbstractOperator&&) = default;

  // abstract method to actually execute the operator
  // execute and get_output are split into two methods to allow for easier
  // asynchronous execution
  virtual void execute() = 0;

  // returns the result of the operator
  virtual std::shared_ptr<Table> get_output() const = 0;

 protected:
  virtual const std::string get_name() const = 0;

  // returns the number of input tables
  // range of values is [0, 2]
  virtual uint8_t get_num_in_tables() const = 0;

  // returns the number of output tables
  // range of values is [0, 1]
  virtual uint8_t get_num_out_tables() const = 0;

  const std::shared_ptr<Table> _input_left, _input_right;
};

// Some operators need an internal implementation class, mostly in cases where
// their execute method depends on a template parameter. An example for this is
// found in table_scan.hpp.
class AbstractOperatorImpl {
 public:
  virtual void execute() = 0;
  virtual std::shared_ptr<Table> get_output() const = 0;
};
}  // namespace opossum
