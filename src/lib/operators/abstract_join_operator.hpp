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
// https://github.com/hyrise/hyrise/wiki/Operator-Join

// We have decided against forwarding MVCC columns in https://github.com/hyrise/hyrise/issues/409

class AbstractJoinOperator : public AbstractReadOnlyOperator {
 public:
  AbstractJoinOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                       const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                       const ColumnIDPair& column_ids, const PredicateCondition predicate_condition);

  JoinMode mode() const;
  const ColumnIDPair& column_ids() const;
  PredicateCondition predicate_condition() const;
  const std::string description(DescriptionMode description_mode) const override;

 protected:
  const JoinMode _mode;
  const ColumnIDPair _column_ids;
  const PredicateCondition _predicate_condition;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  // Some operators need an internal implementation class, mostly in cases where
  // their execute method depends on a template parameter. An example for this is
  // found in join_hash.hpp.
  class AbstractJoinOperatorImpl : public AbstractReadOnlyOperatorImpl {
   public:
    virtual ~AbstractJoinOperatorImpl() = default;
    virtual std::shared_ptr<const Table> _on_execute() = 0;
  };
};

}  // namespace opossum
