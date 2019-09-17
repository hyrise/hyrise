#pragma once

#include <vector>

#include "abstract_rule.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

/**
 * This rule reverts the changes of the PredicateSplitUpRule after other rules have run. It merges multiple
 * PredicateNodes and UnionNodes into single a PredicateNode with a complex expression. This reduces the query runtime
 * if there are many PredicateNodes and UnionNodes because the ExpressionEvaluator is faster in this case.
 *
 * The rule merges predicate chains to conjunctions ("and") and diamonds (below UnionNodes) to disjunctions ("or").
 *
 * EXAMPLE:
 *   TPC-DS query 41 benefits from this rule because the PredicateSplitUpRule creates a huge LQP.
 */
class PredicateMergeRule : public AbstractRule {
 public:
  explicit PredicateMergeRule(const size_t optimization_threshold);
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;

 private:
  std::shared_ptr<AbstractExpression> _merge_subplan(
      const std::shared_ptr<AbstractLQPNode>& begin,
      const std::optional<const std::shared_ptr<AbstractExpression>>& subsequent_expression) const;

  size_t _optimization_threshold;
};

}  // namespace opossum
