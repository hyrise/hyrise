#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

struct ColumnID;

/**
 * Node type to represent common projections, i.e. without any aggregate functionality.
 */
class ProjectionNode : public AbstractASTNode {
 public:
  explicit ProjectionNode(const std::vector<ColumnID>& column_ids);

  std::string description() const override;

  std::vector<ColumnID> output_column_ids() const override;
};

}  // namespace opossum
