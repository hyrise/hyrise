#pragma once

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;

/**
 * A semi join reduction is a semi join that is added to the LQP without being present in the unoptimized LQP. This
 * means that it does not change the final results. Take the following query as an example - it is loosely based on
 * TPC-H 20:
 *
 *   SELECT p_name FROM part p1 WHERE p_size >
 *     (SELECT AVG(p_size) FROM part p2 WHERE p1.p_container = p2.p_container)
 *   AND p1.p_container IN ('SM CASE', 'MD CASE', 'LG CASE')
 *
 * It selects all parts from the part table whose size is greater than the average size of parts sold in that
 * container. However, out of the 40 container types, we only look at three.
 *
 * Before this rule is applied, the LQP might look as follows:
 *
 * [ part p1 ] -> [ Predicate p_container IN (...) ------------> [ Semi Join p1.p_container = p2.p_container
 *                                                              /             AND p1.p_size > AVG(p2.p_size) ]
 * [ part p2 ] -> [ Aggregate AVG(p_size) GROUP BY p_container ]
 *
 * As we can see, part is first fully aggregated, even though 37 container types will become irrelevant later. This
 * rule adds a second semi join (the semi join reduction), which uses the p2 side as the left (reduced) input. While
 * this rule adds the semi join reduction directly below the join, the PredicatePlacementRule will push in below the
 * aggregate. As a result, the LQP after this rule looks like this:
 *
 * [ part p1 ] -> [ Predicate p_container IN (...) ] -----------------------------------------------------------------> ...  // NOLINT
 *                                                   \                                                               /
 * [ part p2 ] --------------------------------------> [ Semi Join p1.p_container = p2.p_container ] -> [ Aggregate ]
 *
 * We call p2 the REDUCED NODE on the reduced side and p1 the REDUCER NODE on the reducing side.
 *
 * A different approach to this would be to propagate predicates across joins. However, our current LQP architecture
 * makes it somewhat difficult to identify which predicates can be propagated, especially when multiple joins come
 * into play. Also, predicates might be based on projections and/or joined columns, which makes propagation even more
 * complex. The approach chosen here is more flexible in that it works independently of the predicate's complexity.
 * However, different from a predicate propagation approach, it does not allow us to prune on the reduced side.
**/

class SemiJoinReductionRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;

  // Defines the minimum selectivity for a semi join reduction to be added. For a candidate location in the LQP with an
  // input cardinality `i`, the output cardinality of the semi join has to be lower than `i * MINIMUM_SELECTIVITY`.
  constexpr static auto MINIMUM_SELECTIVITY = .25;
};

}  // namespace opossum
