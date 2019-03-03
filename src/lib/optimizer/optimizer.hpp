#pragma once

#include <memory>
#include <vector>
#include <algorithm>

namespace opossum {

class AbstractRule;
class AbstractLQPNode;

/**
 * Applies optimization rules to an LQP.
 * On each invocation of optimize(), these Batches are applied in the same order as they were added
 * to the Optimizer.
 *
 * Optimizer::create_default_optimizer() creates the Optimizer with the default rule set.
 */
class Optimizer final {
 public:
  static std::shared_ptr<Optimizer> create_default_optimizer();

  void add_rule(std::unique_ptr<AbstractRule> rule);

  template <class T>
  void remove_rules_of_type() {
    _rules.erase(std::remove_if(_rules.begin(), _rules.end(), [](const std::unique_ptr<AbstractRule>& rule){
      return dynamic_cast<T*>(rule.get()) != nullptr;
    }), _rules.end());
  }

  std::shared_ptr<AbstractLQPNode> optimize(const std::shared_ptr<AbstractLQPNode>& input) const;

 private:
  std::vector<std::unique_ptr<AbstractRule>> _rules;

  void _apply_rule(const AbstractRule& rule, const std::shared_ptr<AbstractLQPNode>& root_node) const;
};

}  // namespace opossum
