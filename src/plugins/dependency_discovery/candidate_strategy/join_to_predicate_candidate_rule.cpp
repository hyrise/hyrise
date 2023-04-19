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
  if (other_stored_table_node && other_stored_table_node->table_name == stored_table_node.table_name) {
    return;
  }

  // Candidates for the OD-based rewrite also require an IND. This IND will be destroyed if there is another predicate
  // on the candidate table. Thus, we only add them if the candidate predicate is the only predicate on the table.
  auto od_candidates = DependencyCandidates{};
  auto predicate_count = size_t{0};

  visit_lqp(subtree_root, [&](const auto& node) {
    if (node->type == LQPNodeType::Union || !expression_evaluable_on_lqp(join_lqp_column_expression, *node)) {
      return LQPVisitation::DoNotVisitInputs;
    }

    if (node->type != LQPNodeType::Predicate) {
      return LQPVisitation::VisitInputs;
    }
    ++predicate_count;

    // When we find a predicate node, we check whether the searched column is filtered in this predicate. If so, it is a
    // valid UCC candidate; if not, continue searching.
    const auto& predicate_node = static_cast<const PredicateNode&>(*node);
    const auto& predicate_expression =
        std::dynamic_pointer_cast<AbstractPredicateExpression>(predicate_node.predicate());

    const auto predicate_condition = predicate_expression->predicate_condition;
    const auto is_equals_predicate = predicate_condition == PredicateCondition::Equals;
    if (!is_equals_predicate && !is_between_predicate_condition(predicate_condition)) {
      return LQPVisitation::VisitInputs;
    }

    auto column_expression = std::shared_ptr<LQPColumnExpression>{};

    if (is_equals_predicate) {
      // Get the column expression, which is not always the left operand.
      auto value_expression = std::shared_ptr<AbstractExpression>{};

      for (const auto& predicate_operand : predicate_expression->arguments) {
        if (predicate_operand->type == ExpressionType::LQPColumn) {
          column_expression = std::static_pointer_cast<LQPColumnExpression>(predicate_operand);
        } else if (predicate_operand->type == ExpressionType::Value) {
          value_expression = predicate_operand;
        }
      }

      if (!column_expression || !value_expression || stored_table_node != *column_expression->original_node.lock()) {
        // The predicate needs to look like column = value or value = column; if not, move on.
        return LQPVisitation::VisitInputs;
      }

      candidates.emplace(
          std::make_shared<UccCandidate>(stored_table_node.table_name, column_expression->original_column_id));

    } else {
      column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(predicate_expression->arguments.front());
      if (!column_expression || !join_key_column_expression || !other_stored_table_node ||
          stored_table_node != *column_expression->original_node.lock()) {
        return LQPVisitation::VisitInputs;
      }

      // Candidates will also be generated for equals predicate.
    }

    if (join_node.join_mode != JoinMode::Semi) {
      candidates.emplace(
          std::make_shared<UccCandidate>(stored_table_node.table_name, join_lqp_column_expression->original_column_id));
    }

    if (!other_stored_table_node) {
      return LQPVisitation::VisitInputs;
    }

    od_candidates.emplace(std::make_shared<OdCandidate>(stored_table_node.table_name,
                                                             join_lqp_column_expression->original_column_id,
                                                             column_expression->original_column_id));

    od_candidates.emplace(std::make_shared<IndCandidate>(
        other_stored_table_node->table_name, join_key_column_expression->original_column_id,
        stored_table_node.table_name, join_lqp_column_expression->original_column_id));

    return LQPVisitation::VisitInputs;
  });

  if (predicate_count == 1 && !od_candidates.empty()) {
    candidates.insert(od_candidates.begin(), od_candidates.end());
  }
}

}  // namespace hyrise
