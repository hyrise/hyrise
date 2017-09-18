#pragma once

#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * This node is used in the Optimizer to have an explicit root node.
 * By that Optimizer Rules don't have to worry whether they change the tree-identifying root node,
 * e.g. by removing a Projection.
 */
class ASTRootNode : public AbstractASTNode {
 public:
  ASTRootNode();

  std::string description() const override;
};

}  // namespace opossum
