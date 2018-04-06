#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "types.hpp"

namespace opossum {

/**
   * This operator joins two tables using one column of each table by performing radix-partition-sort and a merge join.
   * The output is a new table with referenced columns for all columns of the two inputs and filtered pos_lists.
   *
   * As with most operators, we do not guarantee a stable operation with regards to positions -
   * i.e., your sorting order might be disturbed.
   *
   * Note: SortMergeJoin does not support null values in the input at the moment.
   * Note: Cross joins are not supported. Use the product operator instead.
   * Note: Outer joins are only implemented for the equi-join case, i.e. the "=" operator.
   */
class JoinSortMerge : public AbstractJoinOperator {
 public:
  JoinSortMerge(const AbstractOperatorCSPtr left, const AbstractOperatorCSPtr right,
                const JoinMode mode, const ColumnIDPair& column_ids, const PredicateCondition op);

  const std::string name() const override;

 protected:
  TableCSPtr _on_execute() override;
  void _on_cleanup() override;
  AbstractOperatorSPtr _on_recreate(
      const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
      const AbstractOperatorSPtr& recreated_input_right) const override;

  template <typename T>
  class JoinSortMergeImpl;

  std::unique_ptr<AbstractJoinOperatorImpl> _impl;
};

}  // namespace opossum
