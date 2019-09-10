#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

// TODO update comment, include join
class ColumnPruningRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
};

}  // namespace opossum
