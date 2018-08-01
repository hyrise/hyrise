#pragma once

#include <memory>
#include <vector>

#include "optimizer/strategy/rule_batch.hpp"

namespace opossum {

class AbstractRule;
class AbstractLQPNode;

/**
 * Applies optimization rules to an LQP. Rules are organized in RuleBatches which can be added to the Optimizer using
 * add_rule_batch(). On each invocation of optimize(), these Batches are applied in the same order as they were added
 * to the Optimizer.
 *
 * By default, you can use Optimizer::get() to retrieve the global default Optimizer, but it is also possible to create
 * and configure a custom Optimizer.
 */
class Optimizer final {
 public:
  static std::shared_ptr<Optimizer> create_default_optimizer();

  explicit Optimizer(const uint32_t max_num_iterations);

  void add_rule_batch(RuleBatch rule_batch);

  std::shared_ptr<AbstractLQPNode> optimize(const std::shared_ptr<AbstractLQPNode>& input) const;

 private:
  std::vector<RuleBatch> _rule_batches;

  // Rather arbitrary right now, atm all rules should be done after one iteration
  uint32_t _max_num_iterations = 10;

  bool _apply_rule_batch(const RuleBatch& rule_batch, const std::shared_ptr<AbstractLQPNode>& root_node) const;
  bool _apply_rule(const AbstractRule& rule, const std::shared_ptr<AbstractLQPNode>& root_node) const;
};

}  // namespace opossum
