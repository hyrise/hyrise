#pragma once

#include "abstract_rule.hpp"

namespace hyrise {

class AbstractLQPNode;

/**
 * Removes expressions (i.e., columns) that are never or no longer used from the plan.
 * - In StoredTableNodes, we can get rid of all columns that are not used anywhere in the LQP.
 * - In ProjectionNodes, we can get rid of columns that are not part of the result or not used anymore. Example:
 *     SELECT SUM(a + 2) FROM (SELECT a, a + 1 FROM t1) t2
 *   Here, `a + 1` is never actually used and should be pruned.
 * - Joins that emit columns that are never used can be rewritten in some cases (see join_to_semi_join_rule.hpp and
 *   join_to_predicate_rewrite_rule.hpp). We mark the prunable input side of a JoinNode to allow the application of
 *   specific rules for these cases.
 */
class ColumnPruningRule : public AbstractRule {
 public:
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace hyrise
