#pragma once

#include <memory>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractCostEstimator;

/**
 * A rule that brings join operations into a (supposedly) efficient order.
 * Currently only the order of inner joins is modified using a single underlying algorithm, DpCcp.
 *
 * TODO(anybody) Use a different join ordering algorithm, such as GreedyOperatorOrdering, if there are too many join
 *               operations for DPccp to handle efficiently.
 */
class JoinOrderingRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;

 private:
  std::shared_ptr<AbstractLQPNode> _perform_join_ordering_recursively(
      const std::shared_ptr<AbstractLQPNode>& lqp) const;
  void _recurse_to_inputs(const std::shared_ptr<AbstractLQPNode>& lqp) const;
};

}  // namespace opossum
