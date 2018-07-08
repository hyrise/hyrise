#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class AbstractExpression;
class AbstractLQPNode;

/**
 * This optimizer rule looks for Expressions in ProjectionNodes that are calculable at planning time
 * and replaces them with a constant.
 */
class ConstantCalculationRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  void _prune_expression(std::shared_ptr<AbstractExpression>& expression) const;
};

}  // namespace opossum
