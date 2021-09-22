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

  // determine if we need to check both inputs
  std::vector<std::shared_ptr<AbstractLQPNode>> inputs;
  inputs.emplace_back(join_node->right_input());
  if (join_node->join_mode == JoinMode::Inner) {
    inputs.emplace_back(join_node->left_input());
  }

  const auto& predicate = std::dynamic_pointer_cast<AbstractPredicateExpression>(predicates[0]);
  if (!predicate) {
    return {};
  }
  //std::cout << std::endl << lqp_node->description() << std::endl;
  const auto& predicate_arguments = predicate->arguments;
  if (predicate_arguments.size() != 2) return {};
  std::vector<DependencyCandidate> candidates;
  std::vector<std::shared_ptr<const AbstractExpression>> input_expressions(inputs.size());
  //input_expressions.reserve(inputs.size());
  for (auto i = size_t{0}; i < inputs.size(); ++i) {
    // std::cout << "    " << i << std::endl;
    const auto& input = inputs[i];
    for (const auto& expression : predicate_arguments) {
      if (expression->type != ExpressionType::LQPColumn) return {};
      if (!expression_evaluable_on_lqp(expression, *input)) {
        continue;
      }
      input_expressions[i] = expression;
    }
  }
  for (const auto& ex : input_expressions) {
    if (ex->type != ExpressionType::LQPColumn) {

      return {};
    }
  }

  if (input_expressions.size() != 2) {
    //std::cout << "    abort size" << std::endl;
    return {};
  }

  // check for given inputs
  for (auto i = size_t{0}; i < inputs.size(); ++i) {
    const auto& input = inputs[i];
    // find StoredTableNode, ensure that the table is not modified
    std::shared_ptr<AbstractLQPNode> actual_input;
    bool abort = false;
    const auto& mutable_expression = std::const_pointer_cast<AbstractExpression>(input_expressions[i]);
    visit_lqp(input, [&](const auto& node){
      // second can only happen if this is a SemiJoin reducer, ignore subplan
      if (abort || !expression_evaluable_on_lqp(mutable_expression, *node)) {
        return LQPVisitation::DoNotVisitInputs;
      }
      switch (node->type) {
        case LQPNodeType::StoredTable: {
          if (actual_input) {
            abort = true;
          }
          actual_input = node;
        } return LQPVisitation::DoNotVisitInputs;
        case LQPNodeType::Validate: return LQPVisitation::VisitInputs;
        case LQPNodeType::Join: {
          // only allow that table is reduced
          const auto& my_join_node = static_cast<const JoinNode&>(*node);
          if (my_join_node.join_mode == JoinMode::Semi && expression_evaluable_on_lqp(mutable_expression, *my_join_node.left_input())) {
            return LQPVisitation::VisitInputs;
          }
          abort = true;
        } return LQPVisitation::DoNotVisitInputs;
        default:
          abort = true;
          return LQPVisitation::DoNotVisitInputs;
      }
    });
    if (abort || !actual_input) continue;
    // std::cout << " a " << actual_input->description() << std::endl;
    // std::cout << "StoredTable: " << input_expressions[i]->description() << "    " << input_expressions[i == 1 ? 0 : 1]->description()  << std::endl;
    //std::cout << "resolve" << std::endl;
    const auto determinant_column_id = resolve_column_expression(input_expressions.at(i));
    const auto dependent_column_id = resolve_column_expression(input_expressions.at(i == 1 ? 0 : 1));
    if (determinant_column_id == INVALID_TABLE_COLUMN_ID || dependent_column_id == INVALID_TABLE_COLUMN_ID) {
      continue;
    }
    //std::cout << "add " <<  determinant_column_id << " --> " << dependent_column_id << std::endl;
    candidates.emplace_back(TableColumnIDs{determinant_column_id}, TableColumnIDs{dependent_column_id}, DependencyType::Inclusion, priority);
    candidates.emplace_back(TableColumnIDs{determinant_column_id}, TableColumnIDs{}, DependencyType::Unique, priority);
  }
  return candidates;
}

}  // namespace opossum
