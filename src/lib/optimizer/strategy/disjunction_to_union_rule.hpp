#pragma once

#include "abstract_rule.hpp"

namespace opossum {

/**
 * This rule turns PredicateNodes with disjunctive chains (e.g., `PredicateNode(a OR b OR c)`) as their scan expression
 *    into n-1 consecutive UnionNodes that combine the results of n simpler PredicateNodes.
 *    The DisjunctionToUnionRule runs after the PredicateSplitUpRule, so when flattening the top-level logical
 *    expression, we expect one or more top-level expressions that were connected using a disjunction ("or").
 *    Conjunctions ("and") might occur nested within such a top-level expression. As the PredicateSplitUpRule already
 *    ran, they will not be split up into successive PredicateNodes. That might be a thing for the future, if it ever
 *    comes up.
 *
 * Doing so enables other Optimizer rules to process these PredicateNodes.
 *
 * EXAMPLE: TPC-DS query 35.
 *   This rule splits up `EXISTS (...) OR EXISTS (...)` into two expressions that can later be rewritten into two
 *   semi-joins.
 */
class DisjunctionToUnionRule : public AbstractRule {
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;
};

}  // namespace opossum
