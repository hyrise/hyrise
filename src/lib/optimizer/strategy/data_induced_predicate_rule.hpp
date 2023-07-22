#pragma once

#include "abstract_rule.hpp"

namespace hyrise {

class AbstractLQPNode;
class PredicateNode;

/**
 * A data induced predicate is a min, max aggregate with an in-between that is added to the LQP without being present in the unoptimized LQP. This // NOLINT
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
 * rule adds a dip, which uses the p2 side as the left (reducer) input. This rule adds the dip directly below the join.
 * As a result, the LQP after this rule looks like this:
 *
 * [ part p1 ] -> [ Predicate p_container IN (...) ----------------------------------> [ InBetween min and max ] -------> [ Semi Join ... ] // NOLINT
 *                                                                                               /        /             /
 * [ part p2 ] -> [ Aggregate AVG(p_size) GROUP BY p_container ] ------> [ GROUP BY MIN(p_container), MAX(p_container) ]
 *
 * We call p1 the REDUCED NODE on the reduced side and p2 the REDUCER NODE on the reducing side.
**/

class DataInducedPredicateRule : public AbstractRule {
 public:
  std::string name() const override;

  // Defines the minimum selectivity for a dip to be added. For a candidate location in the LQP with an
  // input cardinality `i`, the output cardinality of the semi join has to be lower than `i * MINIMUM_SELECTIVITY`.
  constexpr static auto MINIMUM_SELECTIVITY = .4;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
  static bool _find_predicate(const std::shared_ptr<AbstractLQPNode>& current_node,
                              const std::shared_ptr<AbstractExpression>& join_predicate_expression);
};

}  // namespace hyrise
