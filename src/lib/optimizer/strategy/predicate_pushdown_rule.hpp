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
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) override;

 protected:
  bool _predicate_value_demotable(const std::shared_ptr<PredicateNode>& predicate_node,
                              const std::shared_ptr<AbstractLQPNode>& node) const;
  bool _contained_in_left_subtree(const std::shared_ptr<AbstractLQPNode>& node, const LQPColumnReference& column) const;
  bool _contained_in_right_subtree(const std::shared_ptr<AbstractLQPNode>& node,
                                   const LQPColumnReference& column) const;
  bool _contained_in_node(const std::shared_ptr<AbstractLQPNode>& node, const LQPColumnReference& column) const;
};
}  // namespace opossum
