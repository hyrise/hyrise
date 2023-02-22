#include "join_to_predicate_candidate_rule.hpp"

#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace hyrise {

JoinToPredicateCandidateRule::JoinToPredicateCandidateRule() : AbstractDependencyCandidateRule{LQPNodeType::Join} {}

void JoinToPredicateCandidateRule::apply_to_node(const std::shared_ptr<const AbstractLQPNode>& lqp_node,
                                                 DependencyCandidates& candidates) const {
  const auto& join_node = static_cast<const JoinNode&>(*lqp_node);

  const auto& join_predicates = join_node.join_predicates();
  const auto join_mode = join_node.join_mode;
  const auto prunable_side = join_node.prunable_input_side();

  if ((join_mode != JoinMode::Inner && join_mode != JoinMode::Semi) || !prunable_side || join_predicates.size() != 1) {
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
  if (other_stored_table_node && other_stored_table_node->table_name == stored_table_node.table_name) {
    return;
  }

  visit_lqp(subtree_root, [&](const auto& node) {
    if (node->type == LQPNodeType::Union || !expression_evaluable_on_lqp(join_lqp_column_expression, *node)) {
      return LQPVisitation::DoNotVisitInputs;
    }

    if (node->type != LQPNodeType::Predicate) {
      return LQPVisitation::VisitInputs;
    }

    // When we find a predicate node, we check whether the searched column is filtered in this predicate. If so, it is a
    // valid UCC candidate; if not, continue searching.
    const auto& predicate_node = static_cast<const PredicateNode&>(*node);

    if (const auto& predicate = std::dynamic_pointer_cast<BetweenExpression>(predicate_node.predicate())) {
      const auto& predicate_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->operand());
      if (!predicate_column_expression || !join_key_column_expression || !other_stored_table_node) {
        return LQPVisitation::VisitInputs;
      }

      const auto& other_stored_table_node =
          std::dynamic_pointer_cast<const StoredTableNode>(join_key_column_expression->original_node.lock());
      if (!other_stored_table_node || original_node != predicate_column_expression->original_node.lock()) {
        return LQPVisitation::VisitInputs;
      }

      Assert(stored_table_node != *other_stored_table_node, "diff tables");
      candidates.emplace(
          std::make_shared<UccCandidate>(stored_table_node.table_name, join_lqp_column_expression->original_column_id));

      candidates.emplace(std::make_shared<OdCandidate>(stored_table_node.table_name,
                                                       join_lqp_column_expression->original_column_id,
                                                       predicate_column_expression->original_column_id));

      candidates.emplace(std::make_shared<IndCandidate>(
          other_stored_table_node->table_name, join_key_column_expression->original_column_id,
          stored_table_node.table_name, join_lqp_column_expression->original_column_id));

      return LQPVisitation::VisitInputs;
    }

    // Ensure that we look at a binary predicate expression checking for equality (e.g., a = 'x').
    const auto predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node.predicate());
    if (!predicate || predicate->predicate_condition != PredicateCondition::Equals) {
      return LQPVisitation::VisitInputs;
    }

    // Get the column expression, which is not always the left operand.
    auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->left_operand());
    auto value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->right_operand());
    if (!column_expression) {
      column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->right_operand());
      value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->left_operand());
    }

    if (!column_expression || !value_expression) {
      // The predicate needs to look like column = value or value = column; if not, move on.
      return LQPVisitation::VisitInputs;
    }

    const auto expression_table_node =
        std::dynamic_pointer_cast<const StoredTableNode>(column_expression->original_node.lock());

    // Both columns should be in the same table.
    if (!expression_table_node || *expression_table_node != stored_table_node) {
      return LQPVisitation::VisitInputs;
    }

    candidates.emplace(
        std::make_shared<UccCandidate>(expression_table_node->table_name, column_expression->original_column_id));
    candidates.emplace(
        std::make_shared<UccCandidate>(stored_table_node.table_name, join_lqp_column_expression->original_column_id));

    if (!other_stored_table_node) {
      return LQPVisitation::VisitInputs;
    }

    Assert(*join_lqp_column_expression->original_node.lock() == *column_expression->original_node.lock(), "invalid");
    candidates.emplace(std::make_shared<OdCandidate>(stored_table_node.table_name,
                                                     join_lqp_column_expression->original_column_id,
                                                     column_expression->original_column_id));

    Assert(stored_table_node != *other_stored_table_node, "diff tables");
    candidates.emplace(std::make_shared<IndCandidate>(
        other_stored_table_node->table_name, join_key_column_expression->original_column_id,
        stored_table_node.table_name, join_lqp_column_expression->original_column_id));

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace hyrise
