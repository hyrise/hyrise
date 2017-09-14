#pragma once

#include <memory>

namespace opossum {

class AbstractASTNode;

class AbstractRule {
 public:
  virtual const std::shared_ptr<AbstractASTNode> apply_to(const std::shared_ptr<AbstractASTNode> & node) = 0;

 protected:
  void apply_to_children(const std::shared_ptr<AbstractASTNode> & node);
};

}  // namespace opossum
