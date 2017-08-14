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

  std::vector<ColumnID> output_column_ids() const override;

  std::vector<std::string> output_column_names() const;

  bool find_column_id_for_column_name(std::string & column_name, ColumnID &column_id) override;

private:
  const std::vector<std::string> _output_column_names;
};

}  // namespace opossum
