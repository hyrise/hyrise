#pragma once

#include <memory>

#include "abstract_rule.hpp"

namespace hyrise {

class AbstractCostEstimator;

/**
 * A rule that brings join operations into a (supposedly) efficient order.
 * Currently only the order of inner joins is modified using a single underlying algorithm, DpCcp.
 */
class JoinOrderingRule : public AbstractRule {
 public:
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

 private:
  std::shared_ptr<AbstractLQPNode> _perform_join_ordering_recursively(
      const std::shared_ptr<AbstractLQPNode>& lqp) const;
  void _recurse_to_inputs(const std::shared_ptr<AbstractLQPNode>& lqp) const;
};

}  // namespace hyrise
