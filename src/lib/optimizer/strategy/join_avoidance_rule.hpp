#pragma once

#include "abstract_rule.hpp"

namespace hyrise {

/**
 * A rule that tries to remove unnecessary joins.
 *
 * The rewrite can be correctly done if the following conditions hold:
 * - the join is an inner or a semi join,
 * - one side of the join is not used thereafter,
 * - the join attribute on the unused side is a UCC,
 * - the join attribute on the used side is a foreign key to the unfiltered other join key .
 *
 * There is a dependency to the ColumnPruningRule which discovers and marks unused sides of join nodes. Thus, the
 * ColumnPruningRule must be executed in advance.
 */
class JoinAvoidanceRule : public AbstractRule {
 public:
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace hyrise
