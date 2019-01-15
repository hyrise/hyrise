#include "cost_model_adaptive.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "statistics/table_statistics.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

CostModelAdaptive::CostModelAdaptive(const TableScanCoefficientsPerGroup& table_scan_coefficients,
                                     const JoinCoefficientsPerGroup& join_coefficients,
                                     const std::shared_ptr<AbstractFeatureExtractor>& feature_extractor)
    : _table_scan_models({}), _join_models({}), _feature_extractor(feature_extractor) {
  // Initialize all LinearRegression Models
  for (const auto& [group, coefficients] : table_scan_coefficients) {
    _table_scan_models[group] = std::make_shared<LinearRegressionModel>(coefficients);
  }

  for (const auto& [group, coefficients] : join_coefficients) {
    _join_models[group] = std::make_shared<LinearRegressionModel>(coefficients);
  }
}

Cost CostModelAdaptive::_estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto output_row_count = node->get_statistics()->row_count();
  const auto left_input_row_count = node->left_input() ? node->left_input()->get_statistics()->row_count() : 0.0f;
  const auto right_input_row_count = node->right_input() ? node->right_input()->get_statistics()->row_count() : 0.0f;

  switch (node->type) {
    case LQPNodeType::Join: {
      const auto join_node = std::static_pointer_cast<JoinNode>(node);
      return _predict_join(join_node);
    }

    case LQPNodeType::Sort:
      return left_input_row_count * std::log(left_input_row_count);

    case LQPNodeType::Union: {
      const auto union_node = std::static_pointer_cast<UnionNode>(node);

      switch (union_node->union_mode) {
        case UnionMode::Positions:
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
      std::cout << "In default" << std::endl;
      return left_input_row_count + output_row_count;
  }
}

Cost CostModelAdaptive::_predict_join(const std::shared_ptr<JoinNode>& node) const {
  const auto features = _feature_extractor->extract_features(node);

  const JoinModelGroup group{node->operator_type()};
  const auto model = _join_models.at(group);

  return model->predict(features.to_cost_model_features());
}

Cost CostModelAdaptive::_predict_predicate(const std::shared_ptr<PredicateNode>& node) const {
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
  const auto is_small_table = features.output_is_small_table;

  // find correct LR Model based on data type, first_segment_is_reference_segment, and is_small_table
  const TableScanModelGroup group{OperatorType::TableScan, first_column_data_type,
                                  static_cast<bool>(get<int32_t>(reference_segment)),
                                  static_cast<bool>(get<int32_t>(is_small_table))};

  const auto model = _table_scan_models.at(group);
  return model->predict(features.to_cost_model_features());
}

}  // namespace opossum
