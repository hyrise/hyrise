#pragma once

#include <memory>
#include <vector>

#include "utils/create_ptr_aliases.hpp"

namespace opossum {

class AbstractRule;
class AbstractLQPNode;

enum class RuleBatchExecutionPolicy { Once, Iterative };

// Enables the "grouping" of Optimizer Rules into Batches and the ordering of these Batches.
class RuleBatch final {
 public:
  explicit RuleBatch(RuleBatchExecutionPolicy execution_policy);

  RuleBatchExecutionPolicy execution_policy() const;
  const std::vector<AbstractRuleSPtr>& rules() const;

  void add_rule(const AbstractRuleSPtr& rule);

  bool apply_rules_to(const AbstractLQPNodeSPtr& root_node) const;

 private:
  const RuleBatchExecutionPolicy _execution_policy;
  std::vector<AbstractRuleSPtr> _rules;
};
}  // namespace opossum
