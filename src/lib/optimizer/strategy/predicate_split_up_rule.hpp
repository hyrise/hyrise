#pragma once

#include "abstract_rule.hpp"

namespace opossum {

/**
 * This rule turns PredicateNodes with conjunctive chains (e.g. `PredicateNode(a AND b AND c)`) as their scan expression
 *    into multiple consecutive PredicateNodes (e.g. `PredicateNode(c) -> PredicateNode(b) -> PredicateNode(a)`).
 *
 * Doing so enables other Optimizer rules to process these PredicateNodes and split-up PredicateNodes might take a
 *    faster operator execution path.
 *
 * EXAMPLE: TPC-H query 19.
 *   Without this rule, `p_partkey = l_partkey` would not be available as a join predicate and the predicates on
 *   `l_shipmode` and `l_shipinstruct` could not be pulled below the join.
 */
class PredicateSplitUpRule : public AbstractRule {
  std::string name() const override;
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;
};

}  // namespace opossum
