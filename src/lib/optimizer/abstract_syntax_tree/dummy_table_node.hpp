#pragma once

#include <optional>
#include <string>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * This node type represents a dummy table that is used to project literals.
 * See Projection::DummyTable for more details.
 */
class DummyTableNode : public AbstractLogicalPlanNode {
 public:
  DummyTableNode();

  std::string description() const override;

  const std::vector<std::string>& output_column_names() const override;

 protected:
  std::vector<std::string> _output_column_names;
};

}  // namespace opossum
