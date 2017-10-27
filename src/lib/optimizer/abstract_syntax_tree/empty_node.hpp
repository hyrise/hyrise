#pragma once

#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * This node is returned by the SQLToASTTranslator for everything that doesn't have a return value (i.e., CREATE)
 */
class EmptyNode : public AbstractASTNode {
 public:
  EmptyNode();

  std::string description() const override;
};

}  // namespace opossum
