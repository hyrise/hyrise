#include "projection_node.hpp"

#include <sstream>

#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions):
  AbstractLQPNode(LQPNodeType::Projection), expressions(expressions) {}

std::string ProjectionNode::description() const {
  std::stringstream stream;

  stream << "[Projection] ";

  for (size_t column_idx = 0; column_idx < expressions.size(); ++column_idx) {
    stream << expressions[column_idx]->as_column_name();
    if (column_idx + 1 < expressions.size()) {
      stream << ", ";
    }
  }

  return stream.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& ProjectionNode::output_column_expressions() const {
  return expressions;
}

std::shared_ptr<AbstractLQPNode> ProjectionNode::_shallow_copy_impl(LQPNodeMapping & node_mapping) const {
  return make(expressions_copy_and_adapt_to_different_lqp(expressions, node_mapping));
}

bool ProjectionNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const {
  const auto& rhs_expressions = static_cast<const ProjectionNode&>(rhs).expressions;
  return expressions_equal_to_expressions_in_different_lqp(expressions, rhs_expressions, node_mapping);
}

}  // namespace opossum
