#pragma once

#include <memory>
#include <vector>

namespace opossum {

class AbstractRule;
class AbstractLogicalPlanNode;

/**
 * Applies (currently: all) optimization rules to an AST.
 */
class Optimizer final {
 public:
  static const Optimizer& get();

  Optimizer();

  std::shared_ptr<AbstractLogicalPlanNode> optimize(const std::shared_ptr<AbstractLogicalPlanNode>& input) const;

 private:
  std::vector<std::shared_ptr<AbstractRule>> _rules;

  // Rather arbitrary right now, atm all rules should be done after one iteration
  uint32_t _max_num_iterations = 10;
};

}  // namespace opossum
