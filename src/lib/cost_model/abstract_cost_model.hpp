#pragma once

#include <memory>

#include "cost.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractOperator;
class AbstractCostFeatureProxy;
enum class OperatorType;

/**
 * Interface of an algorithm that predicts Cost for operators.
 *
 * Additionally, it can assume which operator the LQPTranslator will use for an LQPNode and thus estimate the Cost
 * of the LQPNode as well (see estimate_lqp_node_cost()).
 * TODO(anybody) somehow stop to "assume which operator the LQPTranslator will use" and ask the LQPTranslator directly.
 *               Somehow.
 */
class AbstractCostModel {
 public:
  virtual ~AbstractCostModel() = default;

  /**
   * @return the name of the CostModel, e.g. "CostModelClever", "CostModelDumb", etc.
   */
  virtual std::string name() const = 0;

  /**
   * Used e.g., to verify estimations of `estimate_operator_cost()` and `estimate_lqp_node_cost()`
   * @return the actual Cost of an **executed** operator
   */
  virtual Cost get_reference_operator_cost(const std::shared_ptr<AbstractOperator>& op) const = 0;

  /**
   * @return    the estimated Cost of an **executed** operator based on the features of its **executed** input(s) and
   *            the features of its output.
   */
  Cost estimate_operator_cost(const std::shared_ptr<AbstractOperator>& op) const;

  /**
   * @return the Cost of an LQP node based on its input's statistics and its output statistics
   */
  Cost estimate_lqp_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const;

 protected:
  /**
   * Override to implement the actual cost model
   */
  virtual Cost _cost_model_impl(const OperatorType operator_type,
                                const AbstractCostFeatureProxy& feature_proxy) const = 0;
};

}  // namespace opossum
