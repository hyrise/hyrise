#include "join_to_predicate_candidate_rule.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"

namespace opossum {

JoinToPredicateCandidateRule::JoinToPredicateCandidateRule() : AbstractDependencyCandidateRule(LQPNodeType::Join) {}

std::vector<DependencyCandidate> JoinToPredicateCandidateRule::apply_to_node(
    const std::shared_ptr<const AbstractLQPNode>& lqp_node, const size_t priority) const {
  const auto join_node = static_pointer_cast<const JoinNode>(lqp_node);
  if (!(join_node->join_mode == JoinMode::Semi || join_node->join_mode == JoinMode::Inner)) {
    return {};
  }

  const auto& predicates = join_node->join_predicates();
  if (predicates.size() != 1) {
    return {};
  }

  // determine if we need to check both inputsa
  std::vector<std::shared_ptr<AbstractLQPNode>> inputs;
  inputs.emplace_back(join_node->right_input());
  if (join_node->join_mode == JoinMode::Inner) {
    inputs.emplace_back(join_node->left_input());
  }

  const auto& predicate = std::static_pointer_cast<AbstractPredicateExpression>(predicates[0]);
  const auto& predicate_arguments = predicate->arguments;
  std::vector<DependencyCandidate> candidates;

  // check for given inputs
  for (const auto& input : inputs) {
    for (const auto& expression : predicate_arguments) {
      if (!expression_evaluable_on_lqp(expression, *input) || expression->type != ExpressionType::LQPColumn) {
        continue;
      }
      const auto join_column = static_pointer_cast<LQPColumnExpression>(expression);
      const auto join_column_id = resolve_column_expression(expression);
      if (join_column_id == INVALID_TABLE_COLUMN_ID) {
        continue;
      }

      std::vector<DependencyCandidate> my_candidates;
      // add join column as UCC candidate
      if (join_node->join_mode == JoinMode::Inner) {
        my_candidates.emplace_back(TableColumnIDs{join_column_id}, TableColumnIDs{}, DependencyType::Unique, priority);
      }

      bool abort = false;
      visit_lqp(input, [&](const auto& node) {
        switch (node->type) {
          case LQPNodeType::Validate:
            return LQPVisitation::VisitInputs;
          case LQPNodeType::StoredTable:
          case LQPNodeType::StaticTable:
            return LQPVisitation::DoNotVisitInputs;
          case LQPNodeType::Predicate: {
            const auto predicate_node = static_pointer_cast<PredicateNode>(node);
            const auto scan_predicate = predicate_node->predicate();
            const auto predicate_expression = static_pointer_cast<AbstractPredicateExpression>(scan_predicate);
            if (predicate_expression->predicate_condition == PredicateCondition::Equals) {
              const auto scan_inputs = predicate_expression->arguments;
              for (const auto& scan_input : scan_inputs) {
                if (scan_input->type == ExpressionType::LQPColumn) {
                  const auto scan_column_id = resolve_column_expression(scan_input);
                  if (scan_column_id == INVALID_TABLE_COLUMN_ID) {
                    continue;
                  }
                  my_candidates.emplace_back(TableColumnIDs{scan_column_id}, TableColumnIDs{}, DependencyType::Unique,
                                             priority);
                }
              }
            }
            if (is_between_predicate_condition(predicate_expression->predicate_condition)) {
              const auto scan_inputs = predicate_expression->arguments;
              for (const auto& scan_input : scan_inputs) {
                if (scan_input->type == ExpressionType::LQPColumn) {
                  const auto scan_column_id = resolve_column_expression(scan_input);
                  if (scan_column_id == INVALID_TABLE_COLUMN_ID) {
                    continue;
                  }
                  my_candidates.emplace_back(TableColumnIDs{scan_column_id}, TableColumnIDs{join_column_id},
                                             DependencyType::Order, priority);
                }
              }
            }
          }
            return LQPVisitation::VisitInputs;
          default: {
            abort = true;
          }
            return LQPVisitation::DoNotVisitInputs;
        }
      });
      if (!abort) {
        candidates.insert(candidates.end(), std::make_move_iterator(my_candidates.begin()),
                          std::make_move_iterator(my_candidates.end()));
      }
    }
  }
  return candidates;
}

}  // namespace opossum
