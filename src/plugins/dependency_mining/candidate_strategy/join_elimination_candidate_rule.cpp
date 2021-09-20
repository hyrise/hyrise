#include "join_elimination_candidate_rule.hpp"

#include <magic_enum.hpp>

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

JoinEliminationCandidateRule::JoinEliminationCandidateRule() : AbstractDependencyCandidateRule(LQPNodeType::Join) {}

std::vector<DependencyCandidate> JoinEliminationCandidateRule::apply_to_node(
    const std::shared_ptr<const AbstractLQPNode>& lqp_node, const size_t priority) const {
  const auto join_node = static_pointer_cast<const JoinNode>(lqp_node);
  if (join_node->join_mode != JoinMode::Inner && join_node->join_mode != JoinMode::Semi) {
    return {};
  }

  const auto& predicates = join_node->join_predicates();
  if (predicates.size() != 1) {
    return {};
  }

  // determine if we need to check both inputs
  std::vector<std::shared_ptr<AbstractLQPNode>> inputs;
  inputs.emplace_back(join_node->right_input());
  //if (join_node->join_mode == JoinMode::Inner) {
    inputs.emplace_back(join_node->left_input());
  //}


  const auto& predicate = std::static_pointer_cast<AbstractPredicateExpression>(predicates[0]);
  // std::cout << std::endl << lqp_node->description() << std::endl;
  const auto& predicate_arguments = predicate->arguments;
  if (predicate_arguments.size() != 2) return {};
  std::vector<DependencyCandidate> candidates;
  std::vector<std::shared_ptr<const AbstractExpression>> input_expressions(inputs.size());
  //input_expressions.reserve(inputs.size());
  for (auto i = size_t{0}; i < inputs.size(); ++i) {
    // std::cout << "    " << i << std::endl;
    const auto& input = inputs[i];
    for (const auto& expression : predicate_arguments) {
      if (!expression_evaluable_on_lqp(expression, *input) || expression->type != ExpressionType::LQPColumn) {
        continue;
      }
      input_expressions[i] = expression;
    }
  }

  // check for given inputs
  for (auto i = size_t{0}; i < inputs.size(); ++i) {
    const auto& input = inputs[i];
    // std::cout << " i " << input->description() << std::endl;
    const auto actual_input = input->type == LQPNodeType::Validate ? input->left_input() : input;
    // std::cout << " a " << actual_input->description() << std::endl;
    if (actual_input->type != LQPNodeType::StoredTable) continue;
    // std::cout << "StoredTable: " << input_expressions[i]->description() << "    " << input_expressions[i == 1 ? 0 : 1]->description()  << std::endl;
    const auto determinant_column_id = resolve_column_expression(input_expressions[i]);
    const auto dependent_column_id = resolve_column_expression(input_expressions[i == 1 ? 0 : 1]);
    if (determinant_column_id == INVALID_TABLE_COLUMN_ID || dependent_column_id == INVALID_TABLE_COLUMN_ID) {
      continue;
    }
    std::cout << "add " <<  determinant_column_id << " --> " << dependent_column_id << std::endl;
    candidates.emplace_back(TableColumnIDs{determinant_column_id}, TableColumnIDs{dependent_column_id}, DependencyType::Inclusion, priority);
    candidates.emplace_back(TableColumnIDs{determinant_column_id}, TableColumnIDs{}, DependencyType::Unique, priority);
  }
  return candidates;
}

}  // namespace opossum
