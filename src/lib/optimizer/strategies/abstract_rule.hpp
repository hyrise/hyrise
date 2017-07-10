#pragma once

#include <memory>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {
class AbstractRule {
 public:
  virtual std::shared_ptr<AbstractAstNode> apply_rule(std::shared_ptr<AbstractAstNode> node) = 0;
};

}  // namespace opossum
