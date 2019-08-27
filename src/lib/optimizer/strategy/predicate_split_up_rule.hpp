#pragma once

#include "abstract_rule.hpp"

namespace opossum {

// TODO(jj): merge class doc
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
class PredicateSplitUpRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;

 private:
  // TODO(jj): add doc
  bool splitAnd(const std::shared_ptr<AbstractLQPNode>& root) const;
  bool splitOr(const std::shared_ptr<AbstractLQPNode>& root) const;
};

}  // namespace opossum
