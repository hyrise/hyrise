#include "join_elimination_candidate_rule.hpp"

#include <magic_enum.hpp>

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

JoinEliminationCandidateRule::JoinEliminationCandidateRule() : AbstractDependencyCandidateRule(LQPNodeType::Join) {}

std::vector<DependencyCandidate> JoinEliminationCandidateRule::apply_to_node(
    const std::shared_ptr<const AbstractLQPNode>& lqp_node, const size_t priority) const {
  const auto join_node = static_pointer_cast<const JoinNode>(lqp_node);
  if (join_node->join_mode != JoinMode::Inner && join_node->join_mode != JoinMode::Semi) {
    return {};
  }

  // skip semi join reductions
  if (join_node->comment == "Semi Reduction") {
    //std::cout << "red" << std::endl;
    return {};
  }
  if (join_node->comment != "") {
    std::cout << join_node->comment << std::endl;
  }

  const auto& predicates = join_node->join_predicates();
  if (predicates.size() != 1) {
    return {};
  }

  const auto& predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicates[0]);
  if (!predicate) {
    return {};
  }
  const auto inputs = std::vector<std::shared_ptr<AbstractExpression>>{predicate->left_operand(), predicate->right_operand()};
  //std::cout << std::endl << lqp_node->description() << std::endl;
  for (const auto& expression : inputs) {
    if (expression->type != ExpressionType::LQPColumn) return {};
  }
  const auto table_columns = TableColumnIDs{resolve_column_expression(predicate->left_operand()), resolve_column_expression(predicate->right_operand())};
  if (table_columns[0] == INVALID_TABLE_COLUMN_ID || table_columns[1] == INVALID_TABLE_COLUMN_ID) return {};

  std::vector<DependencyCandidate> candidates;
  auto inputs_to_visit = std::vector<uint8_t>{1};
  if (join_node->join_mode == JoinMode::Inner) inputs_to_visit.emplace_back(0);
  // check for given inputs
  for (const auto& i : inputs_to_visit) {
    // find StoredTableNode, ensure that the table is not modified
    std::shared_ptr<StoredTableNode> actual_input;
    const auto& determinant = table_columns[i];
    bool abort = false;
    bool found_input = false;
    const auto input_node = i == 0 ? join_node->left_input() : join_node->right_input();
    visit_lqp(input_node, [&](const auto& node){
      if (abort) return LQPVisitation::DoNotVisitInputs;
      switch (node->type) {
        case LQPNodeType::StoredTable: {
          const auto stored_table_node = static_pointer_cast<StoredTableNode>(node);
          if (stored_table_node->table_name != determinant.table_name) return LQPVisitation::DoNotVisitInputs;
          if (found_input) {
            abort = true;
            return LQPVisitation::DoNotVisitInputs;
          }
          found_input = true;
        } return LQPVisitation::DoNotVisitInputs;
        case LQPNodeType::Validate: return LQPVisitation::VisitInputs;
        case LQPNodeType::Join: {
          // only allow that table is reduced
          const auto& my_join_node = static_cast<const JoinNode&>(*node);
          if (my_join_node.join_mode == JoinMode::Semi) {
            return LQPVisitation::VisitLeftInput;
          }
          abort = true;
        } return LQPVisitation::DoNotVisitInputs;
        default:
          abort = true;
          return LQPVisitation::DoNotVisitInputs;
      }
    });

    if (abort || !found_input) continue;
    const auto& dependent = table_columns[i == 1 ? 0 : 1];
    //std::cout << "add " <<  determinant_column_id << " --> " << dependent_column_id << std::endl;
    candidates.emplace_back(TableColumnIDs{determinant}, TableColumnIDs{dependent}, DependencyType::Inclusion, priority);
    candidates.emplace_back(TableColumnIDs{determinant}, TableColumnIDs{}, DependencyType::Unique, priority);
  }
  return candidates;
}

}  // namespace opossum
