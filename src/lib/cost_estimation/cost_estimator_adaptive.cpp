#include "cost_estimator_adaptive.hpp"

#include "cost_estimation/cost_estimator_coefficient_reader.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<AbstractCostEstimator> CostEstimatorAdaptive::new_instance() const {
  return std::make_shared<CostEstimatorAdaptive>(cardinality_estimator->new_instance());
}

void CostEstimatorAdaptive::initialize(const CoefficientsPerGroup& coefficients) {
  // Initialize all LinearRegression Models
  for (const auto& [group, group_coefficients] : coefficients) {
    _models[group] = std::make_shared<LinearRegressionModel>(group_coefficients);
  }
}

Cost CostEstimatorAdaptive::estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto output_row_count = cardinality_estimator->estimate_cardinality(node);
  const auto left_input_row_count =
      node->left_input() ? cardinality_estimator->estimate_cardinality(node->left_input()) : 0.0f;
  const auto right_input_row_count =
      node->right_input() ? cardinality_estimator->estimate_cardinality(node->right_input()) : 0.0f;

  switch (node->type) {
    case LQPNodeType::Join: {
      const auto join_node = std::static_pointer_cast<JoinNode>(node);
      return _predict_join(join_node);
    }

    case LQPNodeType::Sort:
      return left_input_row_count * std::log(left_input_row_count);

    case LQPNodeType::Union: {
      const auto union_node = std::static_pointer_cast<UnionNode>(node);

      switch (union_node->set_operation_mode) {
        case SetOperationMode::Positions:
          return left_input_row_count * std::log(left_input_row_count) +
                 right_input_row_count * std::log(right_input_row_count);
        default:
          Fail("GCC thinks this is reachable");
      }
    }

    case LQPNodeType::Predicate: {
      const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
      return _predict_predicate(predicate_node);
      // return left_input_row_count * _get_expression_cost_multiplier(predicate_node->predicate()) + output_row_count;
    }

    case LQPNodeType::Mock: {
      return Cost{0.0f};
    }

    default:
      //      std::cout << "In default" << std::endl;
      return left_input_row_count + output_row_count;
  }
}

Cost CostEstimatorAdaptive::_predict_join(const std::shared_ptr<JoinNode>& node) const {
  const auto features = _feature_extractor->extract_features(node);

  const auto left_parent_is_stored_table = std::dynamic_pointer_cast<StoredTableNode>(node->left_input());
  const auto right_parent_is_stored_table = std::dynamic_pointer_cast<StoredTableNode>(node->right_input());
  bool is_referenced = !(left_parent_is_stored_table && right_parent_is_stored_table);
  OperatorType operator_type = node->operator_type();
  const ModelGroup group{operator_type, {}, is_referenced};
  //  std::cout << group << std::endl;
  const auto model = _models.at(group);

  return model->predict(features.to_cost_model_features());
}

Cost CostEstimatorAdaptive::_predict_predicate(const std::shared_ptr<PredicateNode>& node) const {
  const auto left_input_node = node->left_input();
  if (!left_input_node) {
    return Cost{0.0f};
  }

  //  const auto& predicate = node->predicate();
  //  const auto& predicate_arguments = predicate->arguments;
  //  const auto& first_argument = predicate_arguments[0];

  // TODO(Sven): actual data type
  const auto first_column_data_type = DataType::Int;
  const auto features = _feature_extractor->extract_features(node);

  const auto reference_segment = features.table_scan_features.first_column.column_is_reference_segment;

  // find correct LR Model based on data type, first_segment_is_reference_segment, and is_small_table
  const ModelGroup group{OperatorType::TableScan, first_column_data_type,
                         // static_cast<bool>(get<int32_t>(reference_segment)),
                         // static_cast<bool>(get<int32_t>(is_small_table))
                         reference_segment};

  const auto model = _models.at(group);
  return model->predict(features.to_cost_model_features());
}

}  // namespace opossum
