#pragma once

#include <memory>

namespace opossum {

class AbstractASTNode;

class AbstractRule {
 public:
  /**
   * Helper method for applying a single rule to an AST. Creates the temorary ASTRootNode and returns its child
   * after applying the rule
   */
  static std::shared_ptr<AbstractASTNode> apply_rule(const std::shared_ptr<AbstractRule> &rule,
                                                     const std::shared_ptr<AbstractASTNode> &input);

  /**
   * This function applies the concrete Optimizer Rule to an AST.
   * apply_to() is intended to be called recursively by the concrete rule.
   * The optimizer will pass the immutable ASTRootNode to this function.
   * @param root      the root node of this AST
   * @return whether the rule changed the AST, used to stop the optimizers iteration
   */
  virtual bool apply_to(const std::shared_ptr<AbstractASTNode> &root) = 0;

 protected:
  /**
   * Takes a copy of the node because applying this rule to children of this node might remove this node from the tree,
   * which might result in this node being deleted if we don't take a copy of the shared_ptr here.
   */
  bool _apply_to_children(std::shared_ptr<AbstractASTNode> node);
};

}  // namespace opossum
