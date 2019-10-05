#pragma once

#include <vector>

#include "abstract_rule.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

/**
 * The PredicateMergeRule reverts all of the PredicateSplitUpRule's changes by merging multiple PredicateNodes and
 * UnionNodes into a single PredicateNode that has a complex expression. This merging reduces the query runtime if there
 * are many UnionNodes because the ExpressionEvaluator is faster in this case. Therefore, a threshold
 * (minimum_union_count) prevents the PredicateMergeRule from merging only few nodes.
 *
 * The rule merges predicate chains to conjunctions ("and") and diamonds (below UnionNodes) to disjunctions ("or").
 *
 * EXAMPLE:
 *   TPC-DS query 41 benefits from this rule because the PredicateSplitUpRule creates a huge LQP.
 */
class PredicateMergeRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;

  // Simple heuristic: The PredicateMergeRule is more likely to improve the performance for complex LQPs.
  size_t minimum_union_count{10}; // > TPCDS-13

 private:
  void _merge_conjunction(const std::shared_ptr<PredicateNode>& predicate_node) const;
  void _merge_disjunction(const std::shared_ptr<UnionNode>& union_node) const;
};

}  // namespace opossum
