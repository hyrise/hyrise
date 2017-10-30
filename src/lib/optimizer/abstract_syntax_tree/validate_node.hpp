#pragma once

#include <memory>
#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * This node type represents validating tables with the Validate operator.
 */
class ValidateNode : public AbstractASTNode {
 public:
  ValidateNode();

  std::string description() const override;

  std::shared_ptr<AbstractASTNode> clone_subtree() const override;
};

}  // namespace opossum
