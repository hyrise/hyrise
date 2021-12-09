#include "abstract_dependency_candidate_rule.hpp"

namespace opossum {

AbstractDependencyCandidateRule::AbstractDependencyCandidateRule(const LQPNodeType node_type)
    : target_node_type(node_type) {}

std::vector<std::shared_ptr<const AbstractLQPNode>> AbstractDependencyCandidateRule::_inputs_to_visit(
    const std::shared_ptr<const JoinNode>& join_node,
    const std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet>&
        required_expressions_by_node) const {
  std::vector<std::shared_ptr<const AbstractLQPNode>> inputs_to_visit;
  const auto& join_outputs = join_node->outputs();

  const auto add_if_unused = [&](const auto& input) {
    const auto& required_expressions = required_expressions_by_node.at(input);
    if (required_expressions.size() != 1) return;
    const auto& join_column_expression = *required_expressions.begin();
    if (join_column_expression->type != ExpressionType::LQPColumn) return;
    for (const auto& output : join_outputs) {
      const auto& required_expressions_by_output = required_expressions_by_node.at(output);
      for (const auto& required_expression : required_expressions_by_output) {
        if (*required_expression == *join_column_expression) return;
      }
    }
    inputs_to_visit.emplace_back(input);
  };

  add_if_unused(join_node->right_input());
  if (join_node->join_mode == JoinMode::Inner) add_if_unused(join_node->left_input());
  return inputs_to_visit;
}

}  // namespace opossum
