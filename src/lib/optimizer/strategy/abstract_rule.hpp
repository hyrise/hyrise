#pragma once

#include <memory>

namespace opossum {

// potential issues with optimization strategies are documented here:
// https://github.com/hyrise/zweirise/wiki/potential_issues

class AbstractASTNode;

class AbstractRule {
 public:
  virtual const std::shared_ptr<AbstractASTNode> apply_to(const std::shared_ptr<AbstractASTNode> node) = 0;
};

}  // namespace opossum
