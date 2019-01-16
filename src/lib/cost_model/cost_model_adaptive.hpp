#pragma once

#include "abstract_cost_estimator.hpp"

#include "cost_model/feature_extractor/abstract_feature_extractor.hpp"
#include "cost_model/feature_extractor/cost_model_feature_extractor.hpp"
#include "cost_model/linear_regression_model.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "operators/abstract_operator.hpp"
#include "types.hpp"

namespace opossum {

// Maybe better don't put this in namespace opossum?
struct TableScanModelGroup {
  const OperatorType operator_type;
  const DataType data_type;
  const bool is_reference_segment;
  const bool is_small_table;

  bool operator==(const TableScanModelGroup& other) const {
    return (operator_type == other.operator_type && data_type == other.data_type &&
            is_reference_segment == other.is_reference_segment && is_small_table == other.is_small_table);
  }
};

// specialized hash function for unordered_map keys
struct TableScanModelGroupHash {
  std::size_t operator()(const TableScanModelGroup& group) const {
    std::size_t operator_type = std::hash<OperatorType>()(group.operator_type);
    std::size_t data_type = std::hash<DataType>()(group.data_type);
    std::size_t is_reference_segment = std::hash<bool>()(group.is_reference_segment);
    std::size_t is_small_table = std::hash<bool>()(group.is_small_table);

    return operator_type ^ data_type ^ is_reference_segment ^ is_small_table;
  }
};

struct JoinModelGroup {
  const OperatorType operator_type;

  bool operator==(const JoinModelGroup& other) const { return (operator_type == other.operator_type); }
};

// specialized hash function for unordered_map keys
struct JoinModelGroupHash {
  std::size_t operator()(const JoinModelGroup& group) const { return std::hash<OperatorType>()(group.operator_type); }
};

using TableScanCoefficientsPerGroup =
    const std::unordered_map<const TableScanModelGroup, const ModelCoefficients, TableScanModelGroupHash>;
using JoinCoefficientsPerGroup =
    const std::unordered_map<const JoinModelGroup, const ModelCoefficients, JoinModelGroupHash>;
using namespace cost_model;

/**
 * Regression-based Cost Model
 */
class CostModelAdaptive : public AbstractCostEstimator {
 public:
  static const std::shared_ptr<CostModelAdaptive> create_default();

  CostModelAdaptive(const TableScanCoefficientsPerGroup& table_scan_coefficients,
                    const JoinCoefficientsPerGroup& join_coefficients,
                    const std::shared_ptr<AbstractFeatureExtractor>& feature_extractor =
                        std::make_shared<CostModelFeatureExtractor>());

 protected:
  Cost _estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  Cost _predict_predicate(const std::shared_ptr<PredicateNode>& predicate_node) const;
  Cost _predict_join(const std::shared_ptr<JoinNode>& join_node) const;

  std::unordered_map<const TableScanModelGroup, std::shared_ptr<LinearRegressionModel>, TableScanModelGroupHash>
      _table_scan_models;

  std::unordered_map<const JoinModelGroup, std::shared_ptr<LinearRegressionModel>, JoinModelGroupHash> _join_models;

  const std::shared_ptr<cost_model::AbstractFeatureExtractor> _feature_extractor;
};

}  // namespace opossum
