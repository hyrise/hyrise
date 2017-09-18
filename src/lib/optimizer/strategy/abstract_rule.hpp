#pragma once

#include <memory>

#include "optimizer/abstract_syntax_tree/ast_root_node.hpp"

namespace opossum {

class AbstractASTNode;

class AbstractRule {
 public:
  /**
   * This function applies the concrete Optimizer Rule to an AST.
   * @param node      the root node of this AST
   * @return          the new root node of this AST
   */
  const std::shared_ptr<AbstractASTNode> apply_to(const std::shared_ptr<AbstractASTNode> &node);

 protected:
  virtual void _apply_to_impl(const std::shared_ptr<AbstractASTNode> &node) = 0;

  void _apply_to_children(const std::shared_ptr<AbstractASTNode> &node);
};

}  // namespace opossum
