#include "join_to_semi_join_candidate_rule.hpp"

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace hyrise {

JoinToSemiJoinCandidateRule::JoinToSemiJoinCandidateRule() : AbstractDependencyCandidateRule{LQPNodeType::Join} {}

void JoinToSemiJoinCandidateRule::apply_to_node(const std::shared_ptr<const AbstractLQPNode>& lqp_node,
                                                DependencyCandidates& candidates) const {
  const auto& join_node = static_cast<const JoinNode&>(*lqp_node);

  const auto prunable_side = join_node.prunable_input_side();
  const auto& join_predicates = join_node.join_predicates();
  if (join_node.join_mode != JoinMode::Inner || !prunable_side || join_predicates.size() != 1) {
    return;
  }

  const auto& binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates.front());
  Assert(binary_predicate, "JoinNode must have at least one BinaryPredicateExpression.");
  if (binary_predicate->predicate_condition != PredicateCondition::Equals) {
    return;
  }

  const auto& subtree_root = join_node.input(*prunable_side);
  auto candidate_expression = binary_predicate->left_operand();
  if (!expression_evaluable_on_lqp(candidate_expression, *subtree_root)) {
    candidate_expression = binary_predicate->right_operand();
  }

  const auto& lqp_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(candidate_expression);
  if (!lqp_column_expression) {
    return;
  }

  // For JoinToSemiJoinRule, the join column is already interesting for optimization.
  const auto& original_node = lqp_column_expression->original_node.lock();
  if (original_node->type != LQPNodeType::StoredTable) {
    return;
  }

  const auto& stored_table_node = static_cast<const StoredTableNode&>(*original_node);
  candidates.emplace(
      std::make_shared<UccCandidate>(stored_table_node.table_name, lqp_column_expression->original_column_id));
}

}  // namespace hyrise
