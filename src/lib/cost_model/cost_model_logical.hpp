#pragma once

#include "abstract_cost_estimator.hpp"

namespace opossum {

class AbstractExpression;

/**
 * Cost model for logical complexity, i.e., approximate number of tuple accesses
 */
class CostModelLogical : public AbstractCostEstimator {
 protected:
  Cost _estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  static float _get_expression_cost_multiplier(const std::shared_ptr<AbstractExpression>& expression);
};

}  // namespace opossum
