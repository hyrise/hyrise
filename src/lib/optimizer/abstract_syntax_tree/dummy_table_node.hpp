#pragma once

#include <string>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * This node type represents a dummy table that is used to project literals.
 * See Projection::DummyTable for more details.
 */
class DummyTableNode : public AbstractASTNode {
 public:
  explicit DummyTableNode();

  std::string description() const override;

 protected:
  void _on_child_changed() override;
};

}  // namespace opossum
