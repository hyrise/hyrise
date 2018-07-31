#pragma once

#include <memory>
#include <vector>

namespace opossum {

class AbstractRule;
class AbstractLQPNode;

enum class RuleBatchExecutionPolicy { Once, Iterative };

// Enables the "grouping" of Optimizer Rules into Batches and the ordering of these Batches.
class RuleBatch final {
 public:
  explicit RuleBatch(RuleBatchExecutionPolicy execution_policy);

  RuleBatchExecutionPolicy execution_policy() const;
  const std::vector<std::shared_ptr<AbstractRule>>& rules() const;

  void add_rule(const std::shared_ptr<AbstractRule>& rule);

 private:
  const RuleBatchExecutionPolicy _execution_policy;
  std::vector<std::shared_ptr<AbstractRule>> _rules;
};
}  // namespace opossum
