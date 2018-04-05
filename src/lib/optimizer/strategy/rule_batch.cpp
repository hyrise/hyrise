#include "rule_batch.hpp"

#include "abstract_rule.hpp"

namespace opossum {

RuleBatch::RuleBatch(RuleBatchExecutionPolicy execution_policy) : _execution_policy(execution_policy) {}

RuleBatchExecutionPolicy RuleBatch::execution_policy() const { return _execution_policy; }

const std::vector<AbstractRuleSPtr>& RuleBatch::rules() const { return _rules; }

void RuleBatch::add_rule(const AbstractRuleSPtr& rule) { _rules.emplace_back(rule); }

bool RuleBatch::apply_rules_to(const AbstractLQPNodeSPtr& root_node) const {
  auto lqp_changed = false;

  for (auto& rule : _rules) {
    lqp_changed |= rule->apply_to(root_node);
  }

  return lqp_changed;
}

}  // namespace opossum
