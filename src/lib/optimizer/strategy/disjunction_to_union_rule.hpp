#pragma once

#include "abstract_rule.hpp"

namespace opossum {

/**
 * This rule turns PredicateNodes with disjunctive chains (e.g., `PredicateNode(a OR b OR c)`) as their scan expression
 *    into n-1 consecutive UnionNodes that combine the results of n PredicateNodes with simple expressions.
 *    This rule is similar to the PredicateSplitUpRule, which handles ANDs instead of ORs. The DisjunctionToUnionRule
 *    runs after the PredicateSplitUpRule, so that expressions that are not in conjunctive normal form (ANDs of ORs)
 *    cannot be fully handled right now. Because the PredicateSplitUpRule already ran, the DisjunctionToUnionRule
 *    assumes that no ANDs are left in the PredicateExpressions.
 *
 * Doing so enables other Optimizer rules to process these PredicateNodes.
 *
 * EXAMPLE: TPC-DS query 35.
 *   Without this rule `EXISTS (...) OR EXISTS (...)` would not be split up and therefore would not be reformulated to
 *   two semi-joins.
 */
class DisjunctionToUnionRule : public AbstractRule {
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;
};

}  // namespace opossum
