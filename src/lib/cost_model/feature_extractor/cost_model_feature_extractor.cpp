#include "cost_model_feature_extractor.hpp"

#include "cost_model/feature_extractor/column_feature_extractor.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_join_predicate.hpp"
#include "statistics/table_statistics.hpp"

namespace opossum {
namespace cost_model {

const CostModelFeatures CostModelFeatureExtractor::extract_features(
    const std::shared_ptr<const AbstractLQPNode>& node) const {
  auto calibration_result = _extract_general_features(node);
  calibration_result.constant_hardware_features = _extract_constant_hardware_features();
  calibration_result.runtime_hardware_features = _extract_runtime_hardware_features();

  auto node_type = node->type;
  //
  // TODO(Sven): add test
  switch (node_type) {
    case LQPNodeType::Predicate: {
      const auto table_scan_node = std::static_pointer_cast<const PredicateNode>(node);
      calibration_result.table_scan_features = _extract_features(table_scan_node);
      break;
    }
    case LQPNodeType::Projection: {
      const auto projection_node = std::static_pointer_cast<const ProjectionNode>(node);
      calibration_result.projection_features = _extract_features(projection_node);
      break;
    }
    case LQPNodeType::Join: {
      const auto join_node = std::static_pointer_cast<const JoinNode>(node);
      calibration_result.join_features = _extract_features(join_node);
      break;
    }
    case LQPNodeType::Aggregate: {
      const auto aggregate_node = std::static_pointer_cast<const AggregateNode>(node);
      calibration_result.aggregate_features = _extract_features(aggregate_node);
      break;
    }
    case LQPNodeType::StoredTable:
      // No need to add specific features
      break;
    default: {
      std::cout << "Unhandled LQP node type in CostModelFeatureExtractor: " << lqp_node_type_to_string.at(node_type)
                << std::endl;
    }
  }

  return calibration_result;
}

const CostModelFeatures CostModelFeatureExtractor::_extract_general_features(
    const std::shared_ptr<const AbstractLQPNode>& node) const {
  CostModelFeatures operator_features{};
  //            const auto time = op->performance_data().walltime;
  //            const auto execution_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time).count();
  //
  //            operator_features.execution_time_ns = execution_time_ns;
  operator_features.operator_type = node->operator_type();
  // Mainly for debug purposes
  //            operator_features.operator_description = op->description(DescriptionMode::SingleLine);

  // Inputs
  if (node->left_input()) {
    const auto left_input = node->left_input();
    operator_features.left_input_row_count = static_cast<size_t>(left_input->get_statistics()->row_count());
    //                operator_features.left_input_chunk_count = left_input->chunk_count();
    //                operator_features.left_input_memory_usage_bytes = left_input->estimate_memory_usage();
    //                operator_features.left_input_chunk_size = left_input->max_chunk_size();
  }

  if (node->right_input()) {
    const auto right_input = node->right_input();
    operator_features.right_input_row_count = static_cast<size_t>(right_input->get_statistics()->row_count());
    //                operator_features.right_input_chunk_count = right_input->chunk_count();
    //                operator_features.right_input_memory_usage_bytes = right_input->estimate_memory_usage();
    //                operator_features.right_input_chunk_size = right_input->max_chunk_size();
  }

  const auto left_input_row_count = operator_features.left_input_row_count;
  const auto right_input_row_count = operator_features.right_input_row_count;

  if (left_input_row_count > 0 && right_input_row_count > 0) {
    if (left_input_row_count > right_input_row_count) {
      operator_features.input_table_size_ratio = left_input_row_count / static_cast<float>(right_input_row_count);
    } else {
      operator_features.input_table_size_ratio = right_input_row_count / static_cast<float>(left_input_row_count);
    }
  }

  operator_features.logical_cost_sort_merge = left_input_row_count * static_cast<float>(std::log(left_input_row_count)) + right_input_row_count * static_cast<float>(std::log(right_input_row_count));
  operator_features.logical_cost_hash = left_input_row_count + right_input_row_count;

  // Output selectivity seems to be off
  const auto output_row_count = static_cast<size_t>(node->get_statistics()->row_count());
  // Calculate cross-join cardinality.
  // Use 1 for cases, in which one side is empty to avoid divisions by zero in the next step
  const auto total_input_row_count =
      std::max<uint64_t>(1, left_input_row_count) * std::max<uint64_t>(1, right_input_row_count);
  const auto selectivity = std::min<float>(1.0, output_row_count / static_cast<float>(total_input_row_count));

  operator_features.total_row_count = total_input_row_count;
  operator_features.selectivity = selectivity;
  operator_features.output_row_count = output_row_count;
  //            operator_features.output_chunk_count = output->chunk_count();
  //            operator_features.output_memory_usage_bytes = output->estimate_memory_usage();

  //            const auto output_chunk_size = output->max_chunk_size();
  //            operator_features.output_chunk_size = output_chunk_size;

  return operator_features;
}

const ConstantHardwareFeatures CostModelFeatureExtractor::_extract_constant_hardware_features() const { return {}; }

const RuntimeHardwareFeatures CostModelFeatureExtractor::_extract_runtime_hardware_features() const { return {}; }

const TableScanFeatures CostModelFeatureExtractor::_extract_features(
    const std::shared_ptr<const PredicateNode>& node) const {
  TableScanFeatures features{};

  const auto left_input = node->left_input();
  //            auto chunk_count = left_input_table->chunk_count();

  const auto& table_condition = node->predicate();
  features.computable_or_column_expression_count = count_expensive_child_expressions(table_condition);
  //  features.effective_chunk_count = chunk_count - node->get_number_of_excluded_chunks();

  if (table_condition->type == ExpressionType::Predicate) {
    const auto& casted_predicate = std::dynamic_pointer_cast<AbstractPredicateExpression>(table_condition);
    _extract_table_scan_features_for_predicate_expression(left_input, features, casted_predicate);
  } else if (table_condition->type == ExpressionType::Logical) {
    const auto logical_expression = std::dynamic_pointer_cast<LogicalExpression>(table_condition);
    if (logical_expression->logical_operator == LogicalOperator::Or) {
      const auto& casted_predicate = std::dynamic_pointer_cast<LogicalExpression>(table_condition);
      features.scan_operator_type = logical_operator_to_string.left.at(casted_predicate->logical_operator);
    }
  }

  return features;
}

void CostModelFeatureExtractor::_extract_table_scan_features_for_predicate_expression(
    const std::shared_ptr<AbstractLQPNode>& input, TableScanFeatures& features,
    const std::shared_ptr<AbstractPredicateExpression>& expression) const {
  features.scan_operator_type = predicate_condition_to_string.left.at(expression->predicate_condition);

  const auto& predicate_arguments = expression->arguments;

  // TODO(Sven): for now, only column expressions are evaluated as they are expected to be expensive

  // TODO(Sven): This expects a binary or between expression
  if (predicate_arguments.size() == 2 || predicate_arguments.size() == 3) {
    const auto& first_argument = predicate_arguments[0];
    if (first_argument->type == ExpressionType::LQPColumn) {
      const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(first_argument);
      features.first_column = ColumnFeatureExtractor::extract_features(input, column_expression, "first");
    }

    const auto& second_argument = predicate_arguments[1];
    if (second_argument->type == ExpressionType::LQPColumn) {
      features.is_column_comparison = true;

      const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(second_argument);
      features.second_column = ColumnFeatureExtractor::extract_features(input, column_expression, "second");
    }

    if (predicate_arguments.size() == 3) {
      const auto& third_argument = predicate_arguments[2];
      if (third_argument->type == ExpressionType::LQPColumn) {
        const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(third_argument);
        features.third_column = ColumnFeatureExtractor::extract_features(input, column_expression, "third");
      }
    }

  } else {
    std::cout << "facing unexpected table scan with 1 or more than 3 predicates: " << *expression << std::endl;
  }
}

const ColumnFeatures CostModelFeatureExtractor::_extract_features_for_column_expression(
    const std::shared_ptr<AbstractLQPNode>& input, const ColumnID& column_id, const std::string& prefix) const {
  return ColumnFeatures{""};
}

//const std::map<EncodingType, size_t> CostModelFeatureExtractor::_get_encoding_type_for_column(
//    const LQPColumnReference& reference) {
//  const auto original_node = reference.original_node();
//  const auto column_id = reference.original_column_id();
//
//  if (original_node->type != LQPNodeType::StoredTable) {
//    // Only StoredTables can be encoded
//    return EncodingType::Unencoded;
//  }
//
//  const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(original_node);
//  const auto& underlying_table_name = stored_table_node->table_name;

//  auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
//
//  // Dereference ReferenceSegment for encoding feature
//  // TODO(Sven): add test for empty referenced table
//  // TODO(Sven): add test to check for encoded, referenced column
//  if (reference_segment && reference_segment->referenced_table()->chunk_count() > ChunkID{0}) {
//    auto underlying_segment = reference_segment->referenced_table()
//                                  ->get_chunk(ChunkID{0})
//                                  ->get_segment(reference_segment->referenced_column_id());
//    auto encoded_scan_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(underlying_segment);
//    if (encoded_scan_segment) {
//      return std::make_pair(encoded_scan_segment->encoding_type(), true);
//    }
//    return std::make_pair(EncodingType::Unencoded, true);
//  } else {
//    auto encoded_scan_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
//    if (encoded_scan_segment) {
//      return std::make_pair(encoded_scan_segment->encoding_type(), false);
//    }
//    return std::make_pair(EncodingType::Unencoded, false);
//  }
//}

const ProjectionFeatures CostModelFeatureExtractor::_extract_features(
    const std::shared_ptr<const ProjectionNode>& node) const {
  ProjectionFeatures features{};
  // TODO(Sven): Add features that signal whether subselects need to be executed
  features.input_column_count = node->left_input()->node_expressions.size();
  features.output_column_count = node->node_expressions.size();

  return features;
}

const JoinFeatures CostModelFeatureExtractor::_extract_features(const std::shared_ptr<const JoinNode>& node) const {
  JoinFeatures features{};
  //    const auto& left_table = node->left_input();
  //    const auto& right_table = node->right_input();

  //  const auto& join_predicate = node->join_predicate();
  //  Assert(join_predicate, "Need predicate for non Cross Join");

  //    const auto operator_join_predicate =
  //        OperatorJoinPredicate::from_expression(*node->join_predicate(), *node->left_input(), *node->right_input());

  //    const auto column_ids = operator_join_predicate->column_ids;

  //    const auto& left_column_expression = PQPColumnExpression::from_table(*left_table, column_ids.first);
  //    const auto& right_column_expression = PQPColumnExpression::from_table(*left_table, column_ids.second);

  //    features.join_type = node->type();
  features.join_mode = node->join_mode;
  //    features.left_join_column = _extract_features_for_column_expression(left_table, left_column_expression, "left");
  //    features.right_join_column = _extract_features_for_column_expression(right_table, right_column_expression, "right");

  return features;
}

const AggregateFeatures CostModelFeatureExtractor::_extract_features(
    const std::shared_ptr<const AggregateNode>& node) const {
  return {};
}

}  // namespace cost_model
}  // namespace opossum
