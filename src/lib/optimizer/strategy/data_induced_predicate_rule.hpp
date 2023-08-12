#pragma once

#include "abstract_rule.hpp"

namespace hyrise {

class AbstractLQPNode;
class PredicateNode;

/**
 * A data induced predicate is a min, max aggregate with an in-between that is added to the LQP without being present in
 * the unoptimized LQP. It has no impact on the final result of the query but filters tuples earlier out.
 * Take the following query as an example - it is loosely based on a subquery from TPC-H 17:
 *
 * SELECT SUM(l_extendedprice)
 * FROM lineitem
 * JOIN part ON l_partkey = p_partkey
 * WHERE p_brand=‘1’ AND p_container=‘2’
 *
 * It selects all parts from the `part` table whose were p_brand = 1 and p_container = 2. It is then joined on partkey
 * with the lineitem tuples, which is a very large table.
 * The data induced predicate uses information that is available about the part side of the query plan and tries to build
 * a filter from this which can then be pushed on the other side of the join.
 *
 * Before this rule is applied, the LQP might look as follows:
 *
 * [ part p ] -> [ Predicate p_brand = 1 AND p_container = 2 ] ------------> [ Semi Join p.p_partkey = l.l_partkey]
 *                                                                         /
 *                                                             [ lineitem l ]
 *
 * Let's assume, that the statistics regarding the results, that are not filtered out in the predicate have the attribute
 * that all filtered in tuples have a min partkey of 1 and max partkey of 3.
 * If there was a maximum of 20 partkeys then we could filter out 85% of the tuples (if equally distributed) by filtering
 * the lineitem tuples by this predicate.
 * The resulting SQL Query would then look like this:
 *
 * SELECT SUM(l_extendedprice)
 * FROM lineitem
 * JOIN part ON l_partkey = p_partkey
 * WHERE p_brand=‘1’ AND p_container=‘2’
 * AND p_partkey BETWEEN 1 AND 3
 *
 * This between predicate is a data induced predicate (diP) which is added directly below the join. As a result, the LQP
 * after this rule looks like this:
 *
 * [ part p ] -> [ Predicate p_brand = 1 AND p_container = 2 ] ------------> [ Semi Join p.p_partkey = l.l_partkey]
 *                                         \                                  /
 * [ lineitem l ] ---------------------> [ Predicate p_partkey BETWEEN 1 AND 3 ]
 *
 * We call l the REDUCED NODE on the reduced side and p the REDUCER NODE on the reducing side.
**/

class DataInducedPredicateRule : public AbstractRule {
 public:
  std::string name() const override;

  // Defines the minimum selectivity for a diP to be added. For a candidate location in the LQP with an
  // input cardinality `i`, the output cardinality of the semi join has to be lower than `i * MINIMUM_SELECTIVITY`.
  // 0.4 turned out as most effective for the diP-Rule when running TPC-H with scale factor 10, hence it was set at the
  // MINIMUM_SELECTIVITY.
  constexpr static auto MINIMUM_SELECTIVITY = .4;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace hyrise
