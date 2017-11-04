#pragma once

#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * This node is used in the Optimizer to have an explicit root node it can hold onto the tree with.
 *
 * Optimizer rules are not allowed to remove this node or add nodes above it.
 *
 * By that Optimizer Rules don't have to worry whether they change the tree-identifying root node,
 * e.g. by removing the Projection at the top of the tree.
 */
class ASTRootNode : public AbstractASTNode {
 public:
  ASTRootNode();

  std::string description(DescriptionMode mode) const override;
};

}  // namespace opossum
