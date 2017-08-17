#include "projection_node.hpp"

#include <optimizer/expression/expression_node.hpp>
#include <sstream>
#include <string>
#include <vector>

#include "common.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<std::shared_ptr<ExpressionNode>>& column_expressions)
    : AbstractASTNode(ASTNodeType::Projection), _column_expressions(column_expressions) {}

std::string ProjectionNode::description() const {
  std::ostringstream desc;

  desc << "Projection: ";

  for (auto& column : _output_column_names) {
    desc << " " << column;
  }

  return desc.str();
}

std::vector<std::string> ProjectionNode::output_column_names() const { return _output_column_names; }

const std::vector<std::shared_ptr<ExpressionNode>> ProjectionNode::column_expressions() const {
  return _column_expressions;
}

}  // namespace opossum
