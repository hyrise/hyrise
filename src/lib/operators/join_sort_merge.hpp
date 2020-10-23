#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "operator_join_predicate.hpp"
#include "types.hpp"

namespace opossum {

/**
   * This operator joins two tables using one column of each table by performing radix-partition-sort and a merge join.
   * The output is a new table with referenced columns for all columns of the two inputs and filtered pos_lists.
   *
   * As with most operators, we do not guarantee a stable operation with regards to positions -
   * i.e., your previous sorting order might be disturbed:
   *     - The output chunks will be sorted by the join columns.
   *     - The whole output table will not necessarily be entirely sorted by the join columns.
   *     - However, the whole output table will always be clustered by the join columns.
   */
class JoinSortMerge : public AbstractJoinOperator {
 public:
  static bool supports(const JoinConfiguration config);

  JoinSortMerge(const std::shared_ptr<const AbstractOperator>& left,
                const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                const OperatorJoinPredicate& primary_predicate,
                const std::vector<OperatorJoinPredicate>& secondary_predicates = {});

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_cleanup() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  template <typename T>
  class JoinSortMergeImpl;
  template <typename T>
  friend class JoinSortMergeImpl;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;
};

}  // namespace opossum
