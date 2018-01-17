#pragma once

#include <memory>
#include <vector>

#include "optimizer/strategy/rule_batch.hpp"

namespace opossum {

class AbstractRule;
class AbstractLQPNode;

/**
 * Applies (currently: all) optimization rules to an LQP.
 */
class Optimizer final {
 public:
  static const Optimizer& get();

  static Optimizer create_default_optimizer();

  explicit Optimizer(const uint32_t max_num_iterations);

  void add_rule_batch(RuleBatch rule_batch);

  std::shared_ptr<AbstractLQPNode> optimize(const std::shared_ptr<AbstractLQPNode>& input) const;

 private:
  std::vector<RuleBatch> _rule_batches;

  // Rather arbitrary right now, atm all rules should be done after one iteration
  uint32_t _max_num_iterations = 10;
};

}  // namespace opossum
