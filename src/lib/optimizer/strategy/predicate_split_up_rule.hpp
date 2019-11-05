#pragma once

#include <vector>

#include "abstract_rule.hpp"
#include "logical_query_plan/predicate_node.hpp"

namespace opossum {

/**
 * This rule turns PredicateNodes with (nested) conjunctions ("and") and disjunctions ("or")
 *   (e.g., `PredicateNode(a AND (b OR c))`) as their scan expression into an LQP of consecutive PredicateNodes (for the
 *   conjunctions) and UnionNodes (for the disjunctions).
 *
 * Doing so enables other Optimizer rules to process these PredicateNodes and split-up PredicateNodes might take a
 *    faster operator execution path.
 *
 * EXAMPLES:
 *   TPC-H query 19
 *     This rule makes `p_partkey = l_partkey` available as a join predicate and the predicates on `l_shipmode` and
 *     `l_shipinstruct` can be pulled below the join.
 *
 *   TPC-DS query 35
 *     This rule splits up `EXISTS (...) OR EXISTS (...)` into two expressions that can later be rewritten into two
 *     semi-joins.
 */
class PredicateSplitUpRule : public AbstractRule {
 public:
  explicit PredicateSplitUpRule(const bool split_disjunctions = true);
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;

 private:
  void _split_conjunction(const std::shared_ptr<PredicateNode>& predicate_node) const;
  void _split_disjunction(const std::shared_ptr<PredicateNode>& predicate_node) const;

  bool _split_disjunctions;
};

}  // namespace opossum
