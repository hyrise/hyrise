#include "dependent_group_by_candidate_rule.hpp"

#include "logical_query_plan/aggregate_node.hpp"

namespace opossum {

DependentGroupByCandidateRule::DependentGroupByCandidateRule()
    : AbstractDependencyCandidateRule(LQPNodeType::Aggregate) {}

std::vector<DependencyCandidate> DependentGroupByCandidateRule::apply_to_node(
    const std::shared_ptr<const AbstractLQPNode>& lqp_node, const size_t priority,
    const std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet>&
        required_expressions_by_node) const {
  const auto aggregate_node = static_pointer_cast<const AggregateNode>(lqp_node);
  const auto num_group_by_columns = aggregate_node->aggregate_expressions_begin_idx;
  if (num_group_by_columns < 2) {
    return {};
  }


  const auto& node_expressions = aggregate_node->node_expressions;
  // split columns by table to ease validation later on
  TableColumnIDs columns;
  for (auto expression_idx = size_t{0}; expression_idx < num_group_by_columns; ++expression_idx) {
    if (node_expressions[expression_idx]->type != ExpressionType::LQPColumn) {
      continue;
    }
    auto table_column_id = resolve_column_expression(node_expressions[expression_idx]);
    if (table_column_id != INVALID_TABLE_COLUMN_ID) {
      columns.emplace_back(table_column_id);
    }
  }
  if (columns.size() < 2) {
    return {};
  }

  // We still need to perform all aggregations, but omit grouping all columns but one
  // Let's assume hashing/grouping takes half the execution time, then we safe 0.5 * exec. time * frac omitted columns
  const auto omit_column_ratio = 1.0 - (1.0 / static_cast<double>(num_group_by_columns));
  const auto my_priority = static_cast<size_t>(std::lround(0.5 * omit_column_ratio * static_cast<double>(priority)));

  std::vector<DependencyCandidate> candidates;
  // for now, use UCC candidates instead of FD candidates
  for (const auto& column : columns) {
    candidates.emplace_back(TableColumnIDs{column}, TableColumnIDs{}, DependencyType::Unique, my_priority);
  }
  return candidates;
}

}  // namespace opossum
