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
  static std::shared_ptr<AbstractASTNode> apply_rule(const std::shared_ptr<AbstractRule> & rule,
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
  bool _apply_to_children(const std::shared_ptr<AbstractASTNode> &node);
};

}  // namespace opossum
