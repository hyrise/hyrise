#pragma once

#include "abstract_rule.hpp"

namespace hyrise {

/**
 * This optimizer rule finds chains of adjacent PredicateNodes and sorts them based on the expected cost. This cost is
 * logically estimated based on input sizes and selectivities (see CostEstimatorLogical). Thus, predicates with a low
 * cost are executed first to (hopefully) reduce the size of intermediate results. We also treat single-predicate semi-
 * and anti-joins as predicates since they act as a filter for the input relation and can be executed efficiently.
 * However, we weigh their cost a bit higher to account for their execution overhead.
 *
 * Note:
 * For now, this rule only finds adjacent PredicateNodes, meaning that if there is another node, e.g. a ProjectionNode,
 * between two chains of PredicateNodes we won't order all of them, but only each chain separately.
 * A potential optimization would be to ignore certain intermediate nodes, such as ProjectionNode or SortNode, but
 * respect others, such as JoinNode or UnionNode.
 */
class PredicateReorderingRule : public AbstractRule {
 public:
  std::string name() const override;

  // Using a fixed penalty for joins is not optimal. However, penalizing their execution overhead by some means turned
  // out to be a good idea. We keep it simple and use a constant factor, which is derived experimentally. This might be
  // subject to change in the future if we chose different join implementations, but works reasonably well for now.
  // TODO(anyone): Revisit if we substantially change join execution.
  constexpr static auto JOIN_PENALTY = 1.5f;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace hyrise
