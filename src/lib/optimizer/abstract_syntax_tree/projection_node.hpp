#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class ExpressionNode;

/**
 * Node type to represent common projections, i.e. without any aggregate functionality.
 */
class ProjectionNode : public AbstractASTNode {
 public:
  explicit ProjectionNode(const std::vector<std::shared_ptr<ExpressionNode>>& column_expressions);

  std::string description() const override;

  std::vector<std::string> output_column_names() const override;

  const std::vector<std::shared_ptr<ExpressionNode>> column_expressions() const;

 protected:
  std::vector<std::shared_ptr<ExpressionNode>> _column_expressions;
};

}  // namespace opossum
