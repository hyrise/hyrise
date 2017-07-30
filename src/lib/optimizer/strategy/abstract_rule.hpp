#pragma once

#include <memory>

namespace opossum {

class AbstractASTNode;

class AbstractRule {
 public:
  virtual std::shared_ptr<AbstractASTNode> apply_rule(std::shared_ptr<AbstractASTNode> node) = 0;
};

}  // namespace opossum
