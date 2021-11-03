#include "join_elimination_candidate_rule.hpp"

#include <magic_enum.hpp>

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {

JoinEliminationCandidateRule::JoinEliminationCandidateRule() : AbstractDependencyCandidateRule(LQPNodeType::Join) {}

std::vector<DependencyCandidate> JoinEliminationCandidateRule::apply_to_node(
    const std::shared_ptr<const AbstractLQPNode>& lqp_node, const size_t priority, const std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node) const {
  const auto join_node = static_pointer_cast<const JoinNode>(lqp_node);
  if (join_node->join_mode != JoinMode::Inner && join_node->join_mode != JoinMode::Semi) {
    return {};
  }

  // skip semi join reductions
  if (join_node->comment == "Semi Reduction") return {};

  const auto& predicates = join_node->join_predicates();
  if (predicates.size() != 1) return {};

  const auto& predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicates[0]);
  if (!predicate) return {};

  std::unordered_map<std::string, TableColumnID> table_columns;
  for (const auto& expression : {predicate->left_operand(), predicate->right_operand()}) {
    if (expression->type != ExpressionType::LQPColumn) return {};
    auto table_column_id = resolve_column_expression(expression);
    if (table_column_id == INVALID_TABLE_COLUMN_ID) return {};
    table_columns.emplace(table_column_id.table_name, std::move(table_column_id));
  }

  std::vector<DependencyCandidate> candidates;
  const auto& inputs_to_visit = _inputs_to_visit(join_node, required_expressions_by_node);
  // check for given inputs
  for (const auto& input_node : inputs_to_visit) {
    // find StoredTableNode, ensure that the table is not modified
    std::string determinant_name;
    bool abort = false;
    bool found_input = false;
    visit_lqp(input_node, [&](const auto& node) {
      if (abort) return LQPVisitation::DoNotVisitInputs;
      switch (node->type) {
        case LQPNodeType::StoredTable: {
          const auto& stored_table_node = static_cast<const StoredTableNode&>(*node);
          if (table_columns.find(stored_table_node.table_name) == table_columns.cend()) {
            return LQPVisitation::DoNotVisitInputs;
          }
          if (found_input) {
            abort = true;
            return LQPVisitation::DoNotVisitInputs;
          }
          found_input = true;
          determinant_name = stored_table_node.table_name;
        }
          return LQPVisitation::DoNotVisitInputs;
        case LQPNodeType::Validate:
          return LQPVisitation::VisitInputs;
        case LQPNodeType::Join: {
          // only allow that table is reduced
          const auto& my_join_node = static_cast<const JoinNode&>(*node);
          if (my_join_node.join_mode == JoinMode::Semi) {
            return LQPVisitation::VisitLeftInput;
          }
          abort = true;
        }
          return LQPVisitation::DoNotVisitInputs;
        default:
          abort = true;
          return LQPVisitation::DoNotVisitInputs;
      }
    });

    if (abort || !found_input) continue;
    const auto& determinant = table_columns[determinant_name];
    candidates.emplace_back(TableColumnIDs{determinant}, TableColumnIDs{}, DependencyType::Unique, priority);
    for (const auto& [table_name, table_column] : table_columns) {
      if (table_name == determinant_name) continue;
      candidates.emplace_back(TableColumnIDs{determinant}, TableColumnIDs{table_column}, DependencyType::Inclusion,
                              priority);
    }
  }
  return candidates;
}

}  // namespace opossum
