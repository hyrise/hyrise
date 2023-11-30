#pragma once

#include "abstract_rule.hpp"

namespace hyrise {

/**
 * Triggers the loading of column statistics.
 */
class DataLoadingTriggerRule : public AbstractRule {
 public:
  DataLoadingTriggerRule(const bool load_predicates_only);
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

 private:
  bool _load_predicates_only{true};
};

}  // namespace hyrise
