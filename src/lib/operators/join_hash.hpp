#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * This operator joins two tables using one column of each table.
 * The output is a new table with referenced columns for all columns of the two inputs and filtered pos_lists.
 * If you want to filter by multiple criteria, you can chain this operator.
 *
 * As with most operators, we do not guarantee a stable operation with regards to positions -
 * i.e., your sorting order might be disturbed.
 *
 * Note: JoinHash does not support null values at the moment
 *
 * Find more information in our Wiki: https://github.com/hyrise/hyrise/wiki/Radix-Partitioned-and-Hash-Based-Join
 */
class JoinHash : public AbstractJoinOperator {
 public:
  JoinHash(const AbstractOperatorCSPtr left, const AbstractOperatorCSPtr right,
           const JoinMode mode, const ColumnIDPair& column_ids, const PredicateCondition predicate_condition);

  const std::string name() const override;

 protected:
  TableCSPtr _on_execute() override;
  AbstractOperatorSPtr _on_recreate(
      const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
      const AbstractOperatorSPtr& recreated_input_right) const override;
  void _on_cleanup() override;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;

  template <typename LeftType, typename RightType>
  class JoinHashImpl;
};

}  // namespace opossum
