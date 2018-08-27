#pragma once

#include "abstract_cost_model.hpp"

namespace opossum {

/**
 * Logical complexity cost model
 */
class CostModelLogical : public AbstractCostModel {
 protected:
  virtual Cost _estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
