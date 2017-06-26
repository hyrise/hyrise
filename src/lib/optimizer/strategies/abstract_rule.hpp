#pragma once

#include <memory>

#include "optimizer/abstract_syntax_tree/abstract_node.hpp"

namespace opossum {
class AbstractRule {
 public:
  virtual std::shared_ptr<AbstractNode> apply_rule(std::shared_ptr<AbstractNode> node) = 0;
};

}  // namespace opossum
