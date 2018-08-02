#include "rule_batch.hpp"

#include "abstract_rule.hpp"

namespace opossum {

RuleBatch::RuleBatch(RuleBatchExecutionPolicy execution_policy) : _execution_policy(execution_policy) {}

RuleBatchExecutionPolicy RuleBatch::execution_policy() const { return _execution_policy; }

const std::vector<std::shared_ptr<AbstractRule>>& RuleBatch::rules() const { return _rules; }

void RuleBatch::add_rule(const std::shared_ptr<AbstractRule>& rule) { _rules.emplace_back(rule); }

}  // namespace opossum
