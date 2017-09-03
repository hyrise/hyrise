#pragma once

#include <memory>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class Optimizer {
 public:
  static std::shared_ptr<AbstractASTNode> optimize(std::shared_ptr<AbstractASTNode> input);
};

}  // namespace opossum
