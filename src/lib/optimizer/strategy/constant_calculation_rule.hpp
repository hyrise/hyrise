#pragma once


#include "abstract_rule.hpp" // NEEDEDINCLUDE

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
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  void _prune_expression(std::shared_ptr<AbstractExpression>& expression) const;
};

}  // namespace opossum
