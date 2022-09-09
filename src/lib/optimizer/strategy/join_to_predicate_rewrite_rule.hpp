#pragma once

#include "abstract_rule.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"

namespace opossum {

/**
 * A rule that tries to split up joins into local table scans with subquery expressions.
 *
 * The rewrite can be correctly done if the following conditions hold:
 * - one side of the join is not used thereafter
 * - the join attribute on the unused side is a UCC
 * - there is an equals predicate comparing to a constant on an attribute of the unused side
 * - the predicate attribute is a UCC
 *
 * There is a dependency to the ColumnPruningRule which discovers and marks unused sides of join nodes.
 */
class JoinToPredicateRewriteRule : public AbstractRule {
 public:
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
  void gather_rewrite_info(
      const std::shared_ptr<JoinNode>& join_node,
      std::vector<std::tuple<std::shared_ptr<JoinNode>, LQPInputSide, std::shared_ptr<PredicateNode>>>& rewritables)
      const;

  void _perform_rewrite(const std::shared_ptr<JoinNode>& join_node, const LQPInputSide& removable_side,
                        const std::shared_ptr<PredicateNode>& valid_predicate) const;
};

}  // namespace opossum
