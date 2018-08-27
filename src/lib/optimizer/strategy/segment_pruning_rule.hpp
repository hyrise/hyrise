#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * Removes never-referenced base relation cxlumns from the LQP by inserting ProjectionNodes that select only those
 * expressions needed "further up" in the plan.
 *
 * Does NOT eliminate cxlumns added as temporary cxlumns later in the plan or cxlumns that become useless after a
 * certain point in the LQP. *
 * E.g. `SELECT * FROM t WHERE a + 5 > b AND a + 6 > c`: Here `a + 5` and `a + 6` introduce temporary cxlumns that will
 * NOT be removed by this Rule. But it `t` contains a cxlumn "d", which is obviously never used in this query, this
 * cxlumn "d" will be pruned.
 */
class CxlumnPruningRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const override;

 private:
  static ExpressionUnorderedSet _collect_actually_used_cxlumns(const std::shared_ptr<AbstractLQPNode>& lqp);
  static bool _prune_cxlumns_from_leafs(const std::shared_ptr<AbstractLQPNode>& lqp,
                                        const ExpressionUnorderedSet& referenced_cxlumns);
  static void _prune_cxlumns_in_projections(const std::shared_ptr<AbstractLQPNode>& lqp,
                                            const ExpressionUnorderedSet& referenced_cxlumns);
};

}  // namespace opossum
