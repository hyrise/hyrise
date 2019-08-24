#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * Removes never-referenced base relation columns from the LQP by inserting ProjectionNodes that select only those
 * expressions needed "further up" in the plan.
 *
 * Does NOT eliminate columns added as temporary columns later in the plan or columns that become useless after a
 * certain point in the LQP. *
 * E.g. `SELECT * FROM t WHERE a + 5 > b AND a + 6 > c`: Here `a + 5` and `a + 6` introduce temporary columns that will
 * NOT be removed by this Rule. But it `t` contains a column "d", which is obviously never used in this query, this
 * column "d" will be pruned.
 */
class ColumnPruningRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const override;

 private:
  static ExpressionUnorderedSet _collect_actually_used_columns(const std::shared_ptr<AbstractLQPNode>& lqp);
  static void _prune_columns_from_leaves(const std::shared_ptr<AbstractLQPNode>& lqp,
                                         const ExpressionUnorderedSet& referenced_columns);
  static void _prune_columns_in_projections(const std::shared_ptr<AbstractLQPNode>& lqp,
                                            const ExpressionUnorderedSet& referenced_columns);
};

}  // namespace opossum
