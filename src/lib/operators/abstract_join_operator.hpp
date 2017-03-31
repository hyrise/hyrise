#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

// operator to join two tables using one column of each table
// output is a table with reference columns, named with prefix left and right
// to filter by multiple criteria, you can chain the operator

// As with most operators, we do not guarantee a stable operation with regards
// to positions - i.e., your sorting order might be disturbed

// Natural Join is a special case of an inner join without join_columns
// Natural and Cross Join do not enforce column_names
enum JoinMode { Inner, Left, Right, Outer, Cross, Natural, Self };

class AbstractJoinOperator : public AbstractReadOnlyOperator {
 public:
  AbstractJoinOperator(const std::shared_ptr<const AbstractOperator> left,
                       const std::shared_ptr<const AbstractOperator> right,
                       optional<std::pair<std::string, std::string>> column_names, const std::string &op,
                       const JoinMode mode, const std::string &prefix_left, const std::string &prefix_right);

  virtual ~AbstractJoinOperator() = default;

  // copying a operator is not allowed
  AbstractJoinOperator(AbstractJoinOperator const &) = delete;
  AbstractJoinOperator &operator=(const AbstractJoinOperator &) = delete;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  AbstractJoinOperator(AbstractJoinOperator &&) = default;
  AbstractJoinOperator &operator=(AbstractJoinOperator &&) = default;

 protected:
  const std::string _op;
  const JoinMode _mode;
  const std::string _prefix_left;
  const std::string _prefix_right;
  optional<std::pair<std::string, std::string>> _column_names;

  std::shared_ptr<Table> _output_table;

  // Some operators need an internal implementation class, mostly in cases where
  // their execute method depends on a template parameter. An example for this is
  // found in join_nested_loop_a.hpp.
  class AbstractJoinOperatorImpl : public AbstractReadOnlyOperatorImpl {
   public:
    virtual ~AbstractJoinOperatorImpl() = default;
    virtual std::shared_ptr<const Table> on_execute() = 0;
  };
};

}  // namespace opossum
