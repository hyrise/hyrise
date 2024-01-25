#pragma once

#include <memory>
#include <string>

#include "abstract_rule.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

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
 *
 * Furthermore, we found some cases where the cost-based ordering of predicates and semi-joins is not beneficial. In
 * these cases, we push "expensive" predicates below semi-joins.
 * Examples are:
 *   - IS (NOT) NULL or LIKE predicates that are not executed as efficiently as other scans. On a logical basis, we do
 *     not account for execution details.
 *   - Predicates with rather high estimated selectivities (~0.3-0.4) on large inputs combined with a very powerful
 *     semi-join reducer (few tuples) that has a small right input. The penalty factor and the large input outweigh the
 *     cardinality reduction and the scan suffers from large output writing.
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
