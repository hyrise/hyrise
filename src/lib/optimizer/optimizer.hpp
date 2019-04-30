#pragma once

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <vector>


#include "strategy/between_composition_rule.hpp"
#include "strategy/chunk_pruning_rule.hpp"
#include "strategy/column_pruning_rule.hpp"
#include "strategy/expression_reduction_rule.hpp"
#include "strategy/index_scan_rule.hpp"
#include "strategy/insert_limit_in_exists_rule.hpp"
#include "strategy/join_ordering_rule.hpp"
#include "strategy/predicate_placement_rule.hpp"
#include "strategy/predicate_reordering_rule.hpp"
#include "strategy/predicate_split_up_rule.hpp"
#include "strategy/subquery_to_join_rule.hpp"



namespace opossum {

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
    _rules.erase(std::remove_if(
                     _rules.begin(), _rules.end(),
                     [](const std::unique_ptr<AbstractRule>& rule) { return dynamic_cast<T*>(rule.get()) != nullptr; }),
                 _rules.end());
  }

  std::shared_ptr<AbstractLQPNode> optimize(const std::shared_ptr<AbstractLQPNode>& input) const;

  std::vector<std::unique_ptr<AbstractRule>> _rules;

 private:

  void _apply_rule(const AbstractRule& rule, const std::shared_ptr<AbstractLQPNode>& root_node) const;
};

extern std::unordered_map<std::string, bool> optimizer_rule_status;

}  // namespace opossum
