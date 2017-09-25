#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "types.hpp"

namespace opossum {

// operator to join two tables using one column of each table
// output is a table with reference columns
// to filter by multiple criteria, you can chain the operator

// As with most operators, we do not guarantee a stable operation with regards
// to positions - i.e., your sorting order might be disturbed

// find more information about joins in our Wiki:
// https://github.com/hyrise/zweirise/wiki/Operator-Join

class AbstractJoinOperator : public AbstractReadOnlyOperator {
 public:
  AbstractJoinOperator(const std::shared_ptr<const AbstractOperator> left,
                       const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                       const std::pair<ColumnID, ColumnID> &column_ids, const ScanType scan_type);

  JoinMode mode() const;
  const std::pair<ColumnID, ColumnID> &column_ids() const;
  ScanType scan_type() const;

 protected:
  const JoinMode _mode;
  const std::pair<ColumnID, ColumnID> _column_ids;
  const ScanType _scan_type;

  // Some operators need an internal implementation class, mostly in cases where
  // their execute method depends on a template parameter. An example for this is
  // found in join_nested_loop_a.hpp.
  class AbstractJoinOperatorImpl : public AbstractReadOnlyOperatorImpl {
   public:
    virtual ~AbstractJoinOperatorImpl() = default;
    virtual std::shared_ptr<const Table> _on_execute() = 0;
  };
};

}  // namespace opossum
