#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class ProjectionNode : public AbstractAstNode {
 public:
  explicit ProjectionNode(const std::vector<std::string>& column_names);

  std::string description() const override;

  const std::vector<std::string>& output_columns() const override;
};

}  // namespace opossum
