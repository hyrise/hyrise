#pragma once

#include <map>
#include <vector>

#include "abstract_cost_model.hpp"
#include "cost_feature.hpp"

namespace opossum {

/**
 * CostModelRuntime has different models for different kind of TableScans.
 */
enum class CostModelRuntimeTableScanType {
  ColumnValueNumeric,
  ColumnColumnNumeric,
  ColumnValueString,
  ColumnColumnString,
  Like
};

/**
 * Weights of the CostModelRuntime for a particular build type (release, debug)
 */
struct CostModelRuntimeConfig final {
  std::map<CostModelRuntimeTableScanType, CostFeatureWeights> table_scan_models;
  std::map<OperatorType, CostFeatureWeights> other_operator_models;
};

/**
 * Experimental Cost Model that tries to predict the actual runtime in microseconds of an operator. Experiments have
 * shown it to perform only a little better than the much simpler "CostModelLogical"
 *
 * - Currently only support JoinHash, TableScan, UnionPosition and Product, i.e., the most essential operators for
 *      JoinPlans
 * - Calibrated on a specific machine on a specific hyrise code base - so not expected to yield reliable results
 * - For JoinHash - since it shows erratic performance behaviour - only the runtime of some of the operators phases is
 *      being predicted.
 */
class CostModelRuntime : public AbstractCostModel {
 public:
  static CostModelRuntimeConfig create_debug_build_config();
  static CostModelRuntimeConfig create_release_build_config();

  /**
   * @return a CostModelRuntime calibrated on the current build type (debug, release)
   */
  static CostModelRuntimeConfig create_current_build_type_config();

  explicit CostModelRuntime(const CostModelRuntimeConfig& config = create_current_build_type_config());

  std::string name() const override;

  Cost get_reference_operator_cost(const std::shared_ptr<AbstractOperator>& op) const override;

 protected:
  Cost _cost_model_impl(const OperatorType operator_type, const AbstractCostFeatureProxy& feature_proxy) const override;
  Cost _predict_cost(const CostFeatureWeights& feature_weights, const AbstractCostFeatureProxy& feature_proxy) const;

 private:
  CostModelRuntimeConfig _config;
};

}  // namespace opossum
