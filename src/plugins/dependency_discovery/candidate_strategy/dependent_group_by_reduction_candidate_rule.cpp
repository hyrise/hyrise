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
  const auto column_candidates = std::vector<std::shared_ptr<AbstractExpression>>{
      aggregate_node.node_expressions.cbegin(),
      aggregate_node.node_expressions.cbegin() + aggregate_node.aggregate_expressions_begin_idx};

  for (const auto& column_candidate : column_candidates) {
    const auto lqp_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(column_candidate);
    if (!lqp_column_expression) {
      continue;
    }
    // Every ColumnExpression used as a GroupBy expression should be checked for uniqueness.
    const auto& stored_table_node = static_cast<const StoredTableNode&>(*lqp_column_expression->original_node.lock());
    candidates.emplace(
        std::make_shared<UccCandidate>(stored_table_node.table_name, lqp_column_expression->original_column_id));
  }
}

}  // namespace hyrise
