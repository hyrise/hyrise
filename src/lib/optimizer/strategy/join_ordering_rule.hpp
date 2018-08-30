#pragma once

#include <memory>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractCostModel;

class JoinOrderingRule : public AbstractRule {
 public:
  explicit JoinOrderingRule(const std::shared_ptr<AbstractCostModel>& cost_model);

  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;

 private:
  std::shared_ptr<AbstractLQPNode> _perform_join_ordering_recursively(
      const std::shared_ptr<AbstractLQPNode>& lqp) const;
  void _recurse_to_inputs(const std::shared_ptr<AbstractLQPNode>& lqp) const;

  std::shared_ptr<AbstractCostModel> _cost_model;
};

}  // namespace opossum
