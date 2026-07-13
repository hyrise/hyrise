#pragma once

#include <memory>
#include <optional>
#include <utility>

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "utils/assert.hpp"

namespace hyrise {

/**
 * OptimizationContext is used to track metadata and state about the optimization process / resulting LQP during the optimization process. Currently, it only
 * tracks whether the resulting LQP is cacheable or not. If it is not cacheable, the SQLPipeline will not cache the
 * optimized LQP.
 */
struct OptimizationContext {
  enum class ContainsJoin : uint8_t {
    Unknown,
    No,
    Yes,
  };

  void set_not_cacheable() {
    _is_cacheable = false;
  }

  bool is_cacheable() const {
    return _is_cacheable;
  }

  std::shared_ptr<AbstractCostEstimator> cost_estimator;  // The cost estimator used for the optimization.

  std::unique_ptr<OptimizationContext> deep_copy() const {
    return std::make_unique<OptimizationContext>(*this);
  }

  ContainsJoin contains_join{ContainsJoin::Unknown};

 private:
  bool _is_cacheable{true};  // Indicates whether the optimizer can cache the optimized LQP.
};

}  // namespace hyrise
