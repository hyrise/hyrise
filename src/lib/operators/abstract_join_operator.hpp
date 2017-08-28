#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "types.hpp"

namespace opossum {

// operator to join two tables using one column of each table
// output is a table with reference columns, named with prefix left and right
// to filter by multiple criteria, you can chain the operator

// As with most operators, we do not guarantee a stable operation with regards
// to positions - i.e., your sorting order might be disturbed

// Natural Join is a special case of an inner join without join_columns
// Natural and Cross Join do not enforce column_names

class AbstractJoinOperator : public AbstractReadOnlyOperator {
 public:
  AbstractJoinOperator(const std::shared_ptr<const AbstractOperator> left,
                       const std::shared_ptr<const AbstractOperator> right,
                       optional<std::pair<ColumnID, ColumnID>> column_ids, const ScanType scan_type,
                       const JoinMode mode);

  ScanType scan_type() const;
  JoinMode mode() const;
  const optional<std::pair<ColumnID, ColumnID>> &column_ids() const;

 protected:
  const ScanType _scan_type;
  const JoinMode _mode;
  optional<std::pair<ColumnID, ColumnID>> _column_ids;

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
