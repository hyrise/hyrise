#pragma once

#include "abstract_rule.hpp"

namespace opossum {

/**
 * This rule turns PredicateNodes with disjunctive chains (e.g., `PredicateNode(a OR b OR c)`) as their scan expression
 *    into n-1 consecutive UnionNodes that combine the results of n PredicateNodes with simple expressions.
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
