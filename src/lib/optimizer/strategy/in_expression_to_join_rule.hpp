#pragma once

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;

// TODO Doc

class InExpressionToJoinRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
