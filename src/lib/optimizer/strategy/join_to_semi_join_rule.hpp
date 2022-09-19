#pragma once

#include "abstract_rule.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"

namespace hyrise {

/**
 * A rule that tries to transform joins into semi-joins.
 *
 * The rewrite can be correctly done if the following conditions hold:
 * - one side of the join is not used thereafter
 * - the join attribute on the unused side is a UCC
 *
 * There is a dependency to the ColumnPruningRule which discovers and marks unused sides of join nodes.
 */
class JoinToSemiJoinRule : public AbstractRule {
 public:
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace hyrise
