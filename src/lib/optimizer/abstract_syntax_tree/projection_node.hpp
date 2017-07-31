#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * Node type to represent common projections, i.e. without any aggregate functionality.
 */
class ProjectionNode : public AbstractASTNode {
 public:
  explicit ProjectionNode(const std::vector<std::string>& column_names);

  std::string description() const override;

  std::vector<std::string> output_column_names() const override;
};

}  // namespace opossum
