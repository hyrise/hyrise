#include "join_to_semi_candidate_rule.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"

namespace opossum {

JoinToSemiCandidateRule::JoinToSemiCandidateRule() : AbstractDependencyCandidateRule(LQPNodeType::Join) {}

std::vector<DependencyCandidate> JoinToSemiCandidateRule::apply_to_node(
    const std::shared_ptr<const AbstractLQPNode>& lqp_node, const size_t priority, const std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node) const {
  const auto join_node = static_pointer_cast<const JoinNode>(lqp_node);
  if (join_node->join_mode != JoinMode::Inner) return {};

  const auto& predicates = join_node->join_predicates();
  if (predicates.size() != 1) return {};
  const auto& inputs_to_visit = _inputs_to_visit(join_node, required_expressions_by_node);
  std::vector<DependencyCandidate> candidates;
  for (const auto& input : inputs_to_visit) {
    // _inputs_to_visit() ensured that (i) the input has 1 required column (i.e., join column) and the column is not used later
    const auto join_column_id = resolve_column_expression(required_expressions_by_node.at(input).begin());
    if (join_column_id == INVALID_TABLE_COLUMN_ID) continue;
    candidates.emplace_back(TableColumnIDs{join_column_id}, TableColumnIDs{}, DependencyType::Unique, priority);
  }

  return candidates;
}

}  // namespace opossum
