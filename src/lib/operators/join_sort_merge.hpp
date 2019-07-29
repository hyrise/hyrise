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
   * i.e., your sorting order might be disturbed.
   */
class JoinSortMerge : public AbstractJoinOperator {
 public:
  static bool supports(const JoinConfiguration config);

  JoinSortMerge(const std::shared_ptr<const AbstractOperator>& left,
                const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                const OperatorJoinPredicate& primary_predicate,
                const std::vector<OperatorJoinPredicate>& secondary_predicates = {});

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
  template <typename T>
  friend class JoinSortMergeImpl;

  std::unique_ptr<AbstractJoinOperatorImpl> _impl;
};

}  // namespace opossum
