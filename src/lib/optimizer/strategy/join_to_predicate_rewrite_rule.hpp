#pragma once

#include <memory>
#include <string>

#include "abstract_rule.hpp"

namespace hyrise {

/**
 * A rule that tries to split up joins into local table scans with subquery expressions.
 *
 * The rewrite can be correctly done if the following conditions hold:
 * - the join is an inner or a semi join,
 * - one side of the join is not used thereafter,
 * - the join attribute on the unused side is a UCC,
 * - there is an equality predicate comparing an attribute of the unused side with a constant, and
 * - the predicate attribute is a UCC.
 *
 * There is a dependency to the ColumnPruningRule which discovers and marks unused sides of join nodes. Thus, the
 * ColumnPruningRule must be executed in advance.
 */
class JoinToPredicateRewriteRule : public AbstractRule {
 public:
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace hyrise
