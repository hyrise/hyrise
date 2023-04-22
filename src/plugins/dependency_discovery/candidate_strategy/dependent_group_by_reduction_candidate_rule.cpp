#include "dependent_group_by_reduction_candidate_rule.hpp"

#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace hyrise {

DependentGroupByReductionCandidateRule::DependentGroupByReductionCandidateRule()
    : AbstractDependencyCandidateRule{LQPNodeType::Aggregate} {}

void DependentGroupByReductionCandidateRule::apply_to_node(const std::shared_ptr<const AbstractLQPNode>& lqp_node,
                                                           DependencyCandidates& candidates) const {
  const auto& aggregate_node = static_cast<const AggregateNode&>(*lqp_node);
  const auto group_by_column_count = aggregate_node.aggregate_expressions_begin_idx;
  if (group_by_column_count < 2) {
    return;
  }

  const auto column_candidates = std::vector<std::shared_ptr<AbstractExpression>>{
      aggregate_node.node_expressions.cbegin(), aggregate_node.node_expressions.cbegin() + group_by_column_count};
  auto candidate_columns = std::unordered_map<std::string, std::unordered_set<ColumnID>>{};

  for (const auto& column_candidate : column_candidates) {
    const auto lqp_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(column_candidate);
    if (!lqp_column_expression) {
      continue;
    }

    const auto& stored_table_node = static_cast<const StoredTableNode&>(*lqp_column_expression->original_node.lock());
    candidate_columns[stored_table_node.table_name].emplace(lqp_column_expression->original_column_id);
  }

  for (const auto& [table_name, column_ids] : candidate_columns) {
    if (column_ids.size() == 1) {
      candidates.emplace(std::make_shared<UccCandidate>(table_name, *column_ids.cbegin()));
    } else {
      candidates.emplace(std::make_shared<FdCandidate>(table_name, column_ids));
    }
  }
}

}  // namespace hyrise
