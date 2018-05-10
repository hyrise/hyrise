#pragma once

#include "abstract_cost_model.hpp"

namespace opossum {

/**
 * Cost model that returns the rough number of tuple accesses of the Operator.
 *
 * Research (e.g. "How Good Are Query Optimizers, Really?" by Leis et al) suggests very simple CostModels such as this
 * one are "good enough". Especially cardinality estimation has a bigger impact on plan quality by orders of magnitude.
 *
 * Currently costs all Join Operators, TableScans and UnionPositions
 */
class CostModelLogical : public AbstractCostModel {
 public:
  std::string name() const override;

  Cost get_reference_operator_cost(const std::shared_ptr<AbstractOperator>& op) const override;

 protected:
  Cost _cost_model_impl(const OperatorType operator_type, const AbstractCostFeatureProxy& feature_proxy) const override;
};

}  // namespace opossum
