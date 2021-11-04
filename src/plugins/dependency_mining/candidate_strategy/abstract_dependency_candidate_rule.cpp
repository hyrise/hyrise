#include "abstract_dependency_candidate_rule.hpp"

namespace opossum {

AbstractDependencyCandidateRule::AbstractDependencyCandidateRule(const LQPNodeType node_type)
    : target_node_type(node_type) {}


std::vector<std::shared_ptr<const AbstractLQPNode>> AbstractDependencyCandidateRule::_get_nodes_to_visit(const std::shared_ptr<const JoinNode> join_node, const std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet>& required_expressions_by_node) const {
  std::vector<std::shared_ptr<const AbstractLQPNode>> inputs_to_visit;
  const auto join_outputs = join_node->outputs();

  const auto add_if_unused = [&](const auto& input){
    const auto& required_expressions = required_expressions_by_node.at(input);
    if (required_expressions.size() > 1) {
      return;
    }
    const auto column_expression = *required_expressions.begin();
    if (column_expression->type != ExpressionType::LQPColumn) return;
    for (const auto& output : join_outputs) {
      const auto& required_expressions_by_output = required_expressions_by_node.at(output);
      for (const auto& expression : required_expressions_by_output) {
        if (*expression == *column_expression) {
          return;
        }
      }
    }
    inputs_to_visit.emplace_back(input);
  };

  add_if_unused(join_node->right_input());
  if (join_node->join_mode == JoinMode::Inner) add_if_unused(join_node->left_input());
  return inputs_to_visit;
}

}  // namespace opossum
