#pragma once

#include <vector>

#include "abstract_rule.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

/**
 * Merge a subplan that only consists of PredicateNodes and UnionNodes into a single PredicateNode. The
 * subsequent_expression parameter passes the translated expressions to the translation of its children nodes, which
 * allows to add the translated expression of child node before its parent node to the output expression.
 *
 * A subplan consists of linear "chain" and forked "diamond" parts.
 *
 * EXAMPLE:
 *         Step 1                   Step 2                   Step 3                         Step 4
 *
 *           |                        |                        |                              |
 *      ___Union___              ___Union___           Predicate (A OR B)      Predicate ((D AND C) AND (A OR B))
 *    /            \            /           \                  |                              |
 * Predicate (A)   |         Predicate (A)  |                  |
 *    |            |           |            |                  |
 *    |       Predicate (B)    |      Predicate (B)            |
 *    \           /            \          /                    |
 *     Predicate (C)          Predicate (D AND C)     Predicate (D AND C)
 *           |                        |                        |
 *     Predicate (D)
 *           |
 */

/**
 * This rule cleans up any large subplan that the PredicateSplitUpRule created after other optimizer rules have run.
 * For any of these subplans, the PredicateMergeRule reverts all of the PredicateSplitUpRule's changes by merging
 * multiple PredicateNodes and UnionNodes into single a PredicateNode that has a complex expression.
 * This merging reduces the query runtime if there are many PredicateNodes and UnionNodes because the
 * ExpressionEvaluator is faster in this case.
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
  size_t optimization_threshold{10}; // > TPCDS-13

 private:
  void _merge_conjunction(const std::shared_ptr<PredicateNode>& predicate_node) const;
  void _merge_disjunction(const std::shared_ptr<UnionNode>& union_node) const;
};

}  // namespace opossum
