#pragma once

#include "abstract_cost_model.hpp"

namespace opossum {

class AbstractExpression;

/**
 * Logical complexity cost model
 */
class CostModelLogical : public AbstractCostModel {
 protected:
  Cost _estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const override;

  // Number of operations +  number of different columns accessed to judge expression complexity
  static float _get_expression_cost_multiplier(const std::shared_ptr<AbstractExpression>& expression);
};

}  // namespace opossum
