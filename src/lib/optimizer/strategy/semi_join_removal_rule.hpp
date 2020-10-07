#pragma once

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;

class SemiJoinRemovalRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;

  constexpr static auto MINIMUM_SELECTIVITY = .25;
};

}  // namespace opossum
