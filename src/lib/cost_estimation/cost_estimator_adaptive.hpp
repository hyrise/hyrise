#pragma once

#include "abstract_cost_estimator.hpp"

#include "cost_estimation/feature_extractor/abstract_feature_extractor.hpp"
#include "cost_estimation/feature_extractor/cost_estimator_feature_extractor.hpp"
#include "cost_estimation/linear_regression_model.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "operators/abstract_operator.hpp"
#include "types.hpp"

namespace opossum {

// Maybe better don't put this in namespace opossum?
struct ModelGroup {
  const OperatorType operator_type;
  const std::optional<DataType> data_type = {};
  //  const std::optional<EncodingType> encoding_type = {};
  const std::optional<bool> is_reference_segment = {};
  const std::optional<bool> is_small_table = {};

  bool operator==(const ModelGroup& other) const {
    return (operator_type == other.operator_type && data_type == other.data_type &&
            is_reference_segment == other.is_reference_segment && is_small_table == other.is_small_table);
  }
};

// specialized hash function for unordered_map keys
struct ModelGroupHash {
  std::size_t operator()(const ModelGroup& group) const {
    std::size_t operator_type = std::hash<OperatorType>()(group.operator_type);
    std::size_t data_type = std::hash<std::optional<DataType>>()(group.data_type);
    std::size_t is_reference_segment = std::hash<std::optional<bool>>()(group.is_reference_segment);
    std::size_t is_small_table = std::hash<std::optional<bool>>()(group.is_small_table);

    return operator_type ^ data_type ^ is_reference_segment ^ is_small_table;
  }
};

using CoefficientsPerGroup = std::unordered_map<const ModelGroup, const ModelCoefficients, ModelGroupHash>;

using namespace cost_model;

/**
 * Regression-based Cost Model
 */
class CostEstimatorAdaptive : public AbstractCostEstimator {
 public:
  using AbstractCostEstimator::AbstractCostEstimator;

  std::shared_ptr<AbstractCostEstimator> new_instance() const override;

  void initialize(const CoefficientsPerGroup& coefficients);

  Cost estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  Cost _predict_predicate(const std::shared_ptr<PredicateNode>& predicate_node) const;
  Cost _predict_join(const std::shared_ptr<JoinNode>& join_node) const;

  std::unordered_map<const ModelGroup, std::shared_ptr<LinearRegressionModel>, ModelGroupHash> _models;

  const std::shared_ptr<cost_model::AbstractFeatureExtractor> _feature_extractor =
      std::make_shared<CostEstimatorFeatureExtractor>();
};

}  // namespace opossum
