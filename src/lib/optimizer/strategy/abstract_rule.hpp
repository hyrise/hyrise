#pragma once

#include <memory>
#include <string>

namespace opossum {

// potential issues with optimization strategies are documented here:
// https://github.com/hyrise/hyrise/wiki/potential_issues

class AbstractASTNode;

class AbstractRule {
 public:
  virtual std::string name() const = 0;

  /**
   * This function applies the concrete Optimizer Rule to an AST.
   * apply_to() is intended to be called recursively by the concrete rule.
   * The optimizer will pass the immutable ASTRootNode to this function.
   * @return whether the rule changed the AST, used to stop the optimizers iteration
   */
  virtual bool apply_to(const std::shared_ptr<AbstractASTNode>& root) = 0;

 protected:
  /**
   * IMPORTANT: Takes a copy of the node ptr because applying this rule to children of this node might remove this node
   * from the tree, which might result in this node being deleted if we don't take a copy of the shared_ptr here.
   */
  bool _apply_to_children(std::shared_ptr<AbstractASTNode> node);
};

}  // namespace opossum
