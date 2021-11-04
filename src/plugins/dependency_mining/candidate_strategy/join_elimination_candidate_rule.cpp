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
  // std::cout << join_node->description() << std::endl;

  std::vector<DependencyCandidate> candidates;
  const auto& inputs_to_visit = _inputs_to_visit(join_node, required_expressions_by_node);
  // check for given inputs
  for (const auto& input_node : inputs_to_visit) {
    // find StoredTableNode, ensure that the table is not modified
    std::string determinant_name;
    bool abort = false;
    // _inputs_to_visit() ensured that the input has 1 required column expression
    const auto& determinant = resolve_column_expression(*required_expressions_by_node.at(input_node).begin());
    visit_lqp(input_node, [&](const auto& node) {
      if (abort) return LQPVisitation::DoNotVisitInputs;
      switch (node->type) {
        case LQPNodeType::StoredTable:
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

    if (abort) continue;
    candidates.emplace_back(TableColumnIDs{determinant}, TableColumnIDs{}, DependencyType::Unique, priority);
    for (const auto& [table_name, table_column] : table_columns) {
      if (table_name == determinant.table_name) continue;
      candidates.emplace_back(TableColumnIDs{determinant}, TableColumnIDs{table_column}, DependencyType::Inclusion,
                              priority);
    }
  }
  return candidates;
}

}  // namespace opossum
