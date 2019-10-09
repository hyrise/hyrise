#pragma once

#include <vector>

#include "abstract_rule.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

/**
 * The PredicateSplitUpRule might have rewritten disjunctions into complex predicate/union chains. If no rule between
 * the PredicateSplitUpRule and this rule has benefited from these chains, executing them as predicates and unions might
 * be more expensive than having the ExpressionEvaluator run on the original monolithic expression. Controlled by
 * minimum_union_count, this rule reverts the PredicateSplitUpRule's changes by merging multiple PredicateNodes and
 * UnionNodes into single a PredicateNode.
 *
 * EXAMPLE:
 *   TPC-DS query 41 benefits from this rule because the PredicateSplitUpRule creates a huge LQP.
 */
class PredicateMergeRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;

  size_t minimum_union_count{4};

 private:
  void _merge_disjunction(const std::shared_ptr<UnionNode>& union_node) const;
  void _merge_conjunction(const std::shared_ptr<PredicateNode>& predicate_node) const;
};

}  // namespace opossum
