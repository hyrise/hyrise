#pragma once

#include <memory>
#include <utility>

#include "cost_estimation/abstract_cost_estimator.hpp"

namespace hyrise {

/**
 * OptimizationContext is used to track metadata about the optimization process / resulting LQP. Currently, it only
 * tracks whether the resulting LQP is cacheable or not. If it is not cacheable, the SQLPipeline will not cache the
 * optimized LQP.
 */
struct OptimizationContext {
  explicit OptimizationContext(std::shared_ptr<AbstractCostEstimator> init_cost_estimator)
      : cost_estimator{std::move(init_cost_estimator)} {}

  void set_not_cacheable() {
    _is_cacheable = false;
  }

  bool is_cacheable() const {
    return _is_cacheable;
  }

  std::shared_ptr<AbstractCostEstimator> cost_estimator;  // The cost estimator used for the optimization.

 private:
  bool _is_cacheable{true};  // Indicates whether the optimizer can cache the optimized LQP.
};

}  // namespace hyrise
