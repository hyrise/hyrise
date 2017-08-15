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
  // output_column_names is needed for alias support
  explicit ProjectionNode(const std::vector<ColumnID>& column_ids, const std::vector<std::string>& output_column_names);

  std::string description() const override;

  const std::vector<ColumnID> output_column_ids() const override;

  const std::vector<std::string> output_column_names() const override;

  const optional<ColumnID> find_column_id_for_column_identifier(ColumnIdentifier & column_identifier) const override;

};

}  // namespace opossum
