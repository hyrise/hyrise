#include "projection_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/expression/expression_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<std::shared_ptr<ExpressionNode>>& column_expressions)
    : AbstractASTNode(ASTNodeType::Projection), _column_expressions(column_expressions) {
  std::transform(column_expressions.begin(), column_expressions.end(), std::back_inserter(_output_column_names),
                 [](std::shared_ptr<ExpressionNode> expression) -> std::string {
                   switch (expression->type()) {
                     case ExpressionType::ColumnIdentifier:
                       return expression->name();
                     case ExpressionType::FunctionIdentifier:
                       return expression->to_expression_string();
                     default:
                       Fail("Expression is not a supported type");
                       return "";
                   }
                 });
}

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
