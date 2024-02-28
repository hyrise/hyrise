#include "join_avoidance_candidate_rule.hpp"

#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace hyrise {

JoinAvoidanceCandidateRule::JoinAvoidanceCandidateRule() : AbstractDependencyCandidateRule{LQPNodeType::Join} {}

void JoinAvoidanceCandidateRule::apply_to_node(const std::shared_ptr<const AbstractLQPNode>& lqp_node,
                                               DependencyCandidates& candidates) const {
  const auto& join_node = static_cast<const JoinNode&>(*lqp_node);

  // std::cout << "candidate :" << join_node.description() << std::endl;

  const auto& join_predicates = join_node.join_predicates();
  const auto join_mode = join_node.join_mode;
  const auto prunable_side = join_node.prunable_input_side();

  if ((join_mode != JoinMode::Inner && join_mode != JoinMode::Semi) || !prunable_side || join_predicates.size() != 1 ||
      join_node.is_semi_reduction()) {
    return;
  }

  const auto& binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates.front());
  Assert(binary_predicate, "JoinNode must have at least one BinaryPredicateExpression.");
  if (binary_predicate->predicate_condition != PredicateCondition::Equals) {
    return;
  }

  const auto& subtree_root = join_node.input(*prunable_side);
  auto join_key = binary_predicate->left_operand();
  auto other_join_key = binary_predicate->right_operand();
  if (!expression_evaluable_on_lqp(join_key, *subtree_root)) {
    join_key = binary_predicate->right_operand();
    other_join_key = binary_predicate->left_operand();
  }

  const auto& join_lqp_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(join_key);
  if (!join_lqp_column_expression) {
    return;
  }

  const auto& original_node = join_lqp_column_expression->original_node.lock();
  if (!original_node || original_node->type != LQPNodeType::StoredTable) {
    return;
  }
  const auto& stored_table_node = static_cast<const StoredTableNode&>(*original_node);
  const auto& join_key_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(other_join_key);
  const auto& other_stored_table_node =
      join_key_column_expression
          ? std::dynamic_pointer_cast<const StoredTableNode>(join_key_column_expression->original_node.lock())
          : std::shared_ptr<const StoredTableNode>{};

  // No self joins. Comparing the pointers or even the nodes directly does not work: In TPC-DS, we have multiple nodes
  // with different pruned columns.
  if (!other_stored_table_node || other_stored_table_node->table_name == stored_table_node.table_name) {
    return;
  }

  auto can_rewrite = true;

  visit_lqp(subtree_root, [&](const auto& node) {
    // std::cout << "  " << node->description() << std::endl;

    if (!expression_evaluable_on_lqp(join_lqp_column_expression, *node)) {
      return LQPVisitation::DoNotVisitInputs;
    }

    if (!can_rewrite || (node->type != LQPNodeType::Validate && node->type != LQPNodeType::StoredTable &&
                         node->type != LQPNodeType::Join)) {
      can_rewrite = false;
      // std::cout << __FILE__ << ":" << __LINE__ << std::endl;
      return LQPVisitation::DoNotVisitInputs;
    }

    if (node->type == LQPNodeType::Join) {
      const auto& child_join_node = static_cast<const JoinNode&>(*node);
      if (!child_join_node.is_semi_reduction() ||
          !expression_evaluable_on_lqp(join_lqp_column_expression, *node->left_input()) ||
          !expression_evaluable_on_lqp(join_key_column_expression, *node->right_input())) {
        can_rewrite = false;
        return LQPVisitation::DoNotVisitInputs;
      }
    }

    return LQPVisitation::VisitInputs;
  });

  if (!can_rewrite) {
    return;
  }

  // std::cout << join_node << std::endl;

  const auto column_id = join_lqp_column_expression->original_column_id;
  if (join_mode == JoinMode::Inner) {
    candidates.emplace(std::make_shared<UccCandidate>(stored_table_node.table_name, column_id));
  }
  candidates.emplace(std::make_shared<IndCandidate>(other_stored_table_node->table_name,
                                                    join_key_column_expression->original_column_id,
                                                    stored_table_node.table_name, column_id));
}

}  // namespace hyrise
