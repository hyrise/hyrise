#include "join_to_predicate_candidate_rule.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"

namespace opossum {

JoinToPredicateCandidateRule::JoinToPredicateCandidateRule() : AbstractDependencyCandidateRule(LQPNodeType::Join) {}

std::vector<DependencyCandidate> JoinToPredicateCandidateRule::apply_to_node(
    const std::shared_ptr<const AbstractLQPNode>& lqp_node, const std::shared_ptr<const AbstractOperator>& op,
    const size_t priority,
    const std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet>&
        required_expressions_by_node) const {
  const auto join_node = static_pointer_cast<const JoinNode>(lqp_node);
  if (join_node->join_mode != JoinMode::Inner && join_node->join_mode != JoinMode::Semi) {
    return {};
  }

  const auto& predicates = join_node->join_predicates();
  if (predicates.size() != 1) {
    return {};
  }

  // the table could be (massively) pruned by rewriting the join to a scan, increasing the usefulness
  // yet, factor 3 is arbitrarily chosen
  const auto my_priority = 3 * priority;

  // determine if we need to check both inputs
  const auto& inputs_to_visit = _inputs_to_visit(join_node, required_expressions_by_node);

  const auto& predicate = std::static_pointer_cast<BinaryPredicateExpression>(predicates[0]);
  if (!predicate) return {};
  std::unordered_map<std::string, TableColumnID> join_column_ids;
  for (const auto& expression : {predicate->left_operand(), predicate->right_operand()}) {
    if (expression->type != ExpressionType::LQPColumn) return {};
    auto table_column_id = resolve_column_expression(expression);
    if (table_column_id == INVALID_TABLE_COLUMN_ID) return {};
    join_column_ids.emplace(table_column_id.table_name, std::move(table_column_id));
  }

  // check for given inputs
  std::vector<DependencyCandidate> candidates;
  for (const auto& input : inputs_to_visit) {
    std::vector<DependencyCandidate> my_candidates;
    bool abort = false;
    std::string candidate_table;
    visit_lqp(input, [&](const auto& node) {
      switch (node->type) {
        case LQPNodeType::Validate:
          return LQPVisitation::VisitInputs;
        case LQPNodeType::StoredTable:
          return LQPVisitation::DoNotVisitInputs;
        case LQPNodeType::Predicate: {
          const auto& predicate_node = static_cast<const PredicateNode&>(*node);
          const auto scan_predicate = predicate_node.predicate();
          const auto predicate_expression = static_pointer_cast<AbstractPredicateExpression>(scan_predicate);
          if (predicate_expression->predicate_condition == PredicateCondition::Equals) {
            const auto scan_inputs = predicate_expression->arguments;
            for (const auto& scan_input : scan_inputs) {
              if (scan_input->type == ExpressionType::LQPColumn) {
                const auto scan_column_id = resolve_column_expression(scan_input);
                if (scan_column_id == INVALID_TABLE_COLUMN_ID) {
                  continue;
                }
                candidate_table = scan_column_id.table_name;
                my_candidates.emplace_back(TableColumnIDs{scan_column_id}, TableColumnIDs{}, DependencyType::Unique,
                                           my_priority);
                my_candidates.emplace_back(TableColumnIDs{join_column_ids[candidate_table]},
                                           TableColumnIDs{scan_column_id}, DependencyType::Order, my_priority);
              }
            }
          } else if (is_between_predicate_condition(predicate_expression->predicate_condition)) {
            const auto scan_inputs = predicate_expression->arguments;
            for (const auto& scan_input : scan_inputs) {
              if (scan_input->type == ExpressionType::LQPColumn) {
                const auto scan_column_id = resolve_column_expression(scan_input);
                if (scan_column_id == INVALID_TABLE_COLUMN_ID) {
                  continue;
                }
                candidate_table = scan_column_id.table_name;
                my_candidates.emplace_back(TableColumnIDs{join_column_ids[candidate_table]},
                                           TableColumnIDs{scan_column_id}, DependencyType::Order, my_priority);
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

    if (abort || my_candidates.empty()) continue;

    // add join column as UCC candidate
    if (join_node->join_mode == JoinMode::Inner) {
      my_candidates.emplace_back(TableColumnIDs{join_column_ids[candidate_table]}, TableColumnIDs{},
                                 DependencyType::Unique, my_priority);
    }

    candidates.insert(candidates.end(), std::make_move_iterator(my_candidates.begin()),
                      std::make_move_iterator(my_candidates.end()));
  }

  return candidates;
}

}  // namespace opossum
