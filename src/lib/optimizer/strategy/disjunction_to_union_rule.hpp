#pragma once

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

class DisjunctionToUnionRule : public AbstractRule {
 public:
  std::string name() const override;

  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;
};

}  // namespace opossum
