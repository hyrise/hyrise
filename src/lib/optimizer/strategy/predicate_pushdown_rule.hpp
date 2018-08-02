#pragma once

#include <memory>
#include <string>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;
class LQPColumnReference;

// This optimizer rule is responsible for pushing down pradicates in the lqp as much as possible
// to reduce the result set early on. Currently only predicates with exactly one input Node are supported
// Currently supported nodes:
// - Inner join
// - Sort node
class PredicatePushdownRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};
}  // namespace opossum
