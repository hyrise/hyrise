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
  JoinSortMerge(const std::shared_ptr<const AbstractOperator>& left,
                const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                const ColumnIDPair& column_ids, const PredicateCondition op);

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_cleanup() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  template <typename T>
  class JoinSortMergeImpl;

  std::unique_ptr<AbstractJoinOperatorImpl> _impl;
};

}  // namespace opossum
