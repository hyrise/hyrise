#include "cost_model_feature_extractor.hpp"

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/aggregate.hpp"
#include "operators/index_scan.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/reference_segment.hpp"

namespace opossum {

const CalibrationExample CostModelFeatureExtractor::extract_features(
    const std::shared_ptr<const AbstractOperator>& op) {
  CalibrationExample calibration_result{};

  calibration_result.calibration_features = _extract_general_features(op);
  calibration_result.hardware_features = _extract_constant_hardware_features();
  calibration_result.runtime_features = _extract_runtime_hardware_features();

  auto operator_type = op->type();

  // TODO(Sven): add test
  switch (operator_type) {
    case OperatorType::TableScan: {
      auto table_scan_op = std::static_pointer_cast<const TableScan>(op);
      calibration_result.table_scan_features = _extract_features_for_operator(table_scan_op);
      break;
    }
    case OperatorType::IndexScan: {
      auto index_scan_op = std::static_pointer_cast<const IndexScan>(op);
      calibration_result.table_scan_features = _extract_features_for_operator(index_scan_op);
      break;
    }
    case OperatorType::Projection: {
      auto projection_op = std::static_pointer_cast<const Projection>(op);
      calibration_result.projection_features = _extract_features_for_operator(projection_op);
      break;
    }
    case OperatorType::JoinHash:
    case OperatorType::JoinIndex:
    case OperatorType::JoinMPSM:
    case OperatorType::JoinNestedLoop:
    case OperatorType::JoinSortMerge: {
      auto join_op = std::static_pointer_cast<const AbstractJoinOperator>(op);
      calibration_result.join_features = _extract_features_for_operator(join_op);
      break;
    }
    case OperatorType::Aggregate: {
      auto aggregate_op = std::static_pointer_cast<const Aggregate>(op);
      calibration_result.aggregate_features = _extract_features_for_operator(aggregate_op);
      break;
    }
    case OperatorType::GetTable:
      // No need to add specific features
      break;
    default: {
      std::cout << "Unhandled operator type in CostModelFeatureExtractor: " << operator_type_to_string.at(operator_type)
                << std::endl;
    }
  }

  return calibration_result;
}

const CalibrationFeatures CostModelFeatureExtractor::_extract_general_features(
    const std::shared_ptr<const AbstractOperator>& op) {
  CalibrationFeatures operator_features{};
  const auto time = op->performance_data().walltime;
  const auto execution_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time).count();

  operator_features.execution_time_ns = execution_time_ns;
  operator_features.operator_type = operator_type_to_string.at(op->type());
  // Mainly for debug purposes
  operator_features.operator_description = op->description(DescriptionMode::SingleLine);

  // Inputs
  if (op->input_left()) {
    const auto left_input = op->input_left()->get_output();
    operator_features.left_input_row_count = left_input->row_count();
    operator_features.left_input_chunk_count = left_input->chunk_count();
    operator_features.left_input_memory_usage_bytes = left_input->estimate_memory_usage();
    operator_features.left_input_chunk_size = left_input->max_chunk_size();
  }

  if (op->input_right()) {
    const auto right_input = op->input_right()->get_output();
    operator_features.right_input_row_count = right_input->row_count();
    operator_features.right_input_chunk_count = right_input->chunk_count();
    operator_features.right_input_memory_usage_bytes = right_input->estimate_memory_usage();
    operator_features.right_input_chunk_size = right_input->max_chunk_size();
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

  // Output
  if (const auto& output = op->get_output()) {
    // Output selectivity seems to be off
    const auto output_row_count = output->row_count();
    // Calculate cross-join cardinality.
    // Use 1 for cases, in which one side is empty to avoid divisions by zero in the next step
    const auto total_input_row_count =
        std::max<uint64_t>(1, left_input_row_count) * std::max<uint64_t>(1, right_input_row_count);
    const auto output_selectivity = std::min<float>(1.0, output_row_count / static_cast<float>(total_input_row_count));

    operator_features.output_selectivity = output_selectivity;
    operator_features.output_row_count = output_row_count;
    operator_features.output_chunk_count = output->chunk_count();
    operator_features.output_memory_usage_bytes = output->estimate_memory_usage();

    const auto output_chunk_size = output->max_chunk_size();
    operator_features.output_chunk_size = output_chunk_size;
  }

  return operator_features;
}

const CalibrationConstantHardwareFeatures CostModelFeatureExtractor::_extract_constant_hardware_features() {
  CalibrationConstantHardwareFeatures hardware_features{};
  return hardware_features;
}

const CalibrationRuntimeHardwareFeatures CostModelFeatureExtractor::_extract_runtime_hardware_features() {
  CalibrationRuntimeHardwareFeatures runtime_features{};
  return runtime_features;
}

const std::optional<CalibrationTableScanFeatures> CostModelFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const TableScan>& op) {
  CalibrationTableScanFeatures features{};

  auto left_input_table = op->input_table_left();
  auto chunk_count = left_input_table->chunk_count();

  const auto& table_condition = op->predicate();
  features.number_of_computable_or_column_expressions = count_expensive_child_expressions(table_condition);
  features.number_of_effective_chunks = chunk_count - op->get_number_of_excluded_chunks();

  if (table_condition->type == ExpressionType::Predicate) {
    const auto& casted_predicate = std::dynamic_pointer_cast<AbstractPredicateExpression>(table_condition);
    _extract_table_scan_features_for_predicate_expression(left_input_table, features, casted_predicate);
  } else if (table_condition->type == ExpressionType::Logical) {
    const auto logical_expression = std::dynamic_pointer_cast<LogicalExpression>(table_condition);
    if (logical_expression->logical_operator == LogicalOperator::Or) {
      const auto& casted_predicate = std::dynamic_pointer_cast<LogicalExpression>(table_condition);
      features.scan_operator_type = logical_operator_to_string.left.at(casted_predicate->logical_operator);
      //          const auto& predicate_arguments = casted_predicate->arguments;
    }
  }

  return features;
}

const std::optional<CalibrationTableScanFeatures> CostModelFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const IndexScan>& op) {
  CalibrationTableScanFeatures features{};

  auto left_input_table = op->input_table_left();
  const auto left_column_ids = op->left_columns_ids();
  const auto& predicate_condition = op->predicate_condition();
  const auto predicate_condition_pointer = std::make_shared<PredicateCondition>(predicate_condition);

  features.scan_operator_type = predicate_condition_to_string.left.at(predicate_condition);
  features.number_of_effective_chunks = op->get_number_of_included_chunks();

  DebugAssert(left_column_ids.size() == 1, "Expected only one column for IndexScan in FeatureExtractor");
  const auto column_expression = PQPColumnExpression::from_table(*left_input_table, left_column_ids.front());
  features.first_column = _extract_features_for_column_expression(left_input_table, column_expression);

  return features;
}

CalibrationColumnFeatures CostModelFeatureExtractor::_extract_features_for_column_expression(
    const std::shared_ptr<const Table>& left_input_table, const std::shared_ptr<PQPColumnExpression> column_expression) {
  auto chunk_count = left_input_table->chunk_count();
  const auto& column_id = column_expression->column_id;

  // TODO(Sven): What shall we do when there are different encodings across different chunks?
  if (chunk_count > ChunkID{0}) {
    const auto segment = left_input_table->get_chunk(ChunkID{0})->get_segment(column_id);

    const auto encoding_reference_pair = _get_encoding_type_for_segment(segment);

    return CalibrationColumnFeatures{encoding_type_to_string.left.at(encoding_reference_pair.first),
                                     encoding_reference_pair.second,
                                     data_type_to_string.left.at(column_expression->data_type()),
                                     _get_memory_usage_for_column(left_input_table, column_id)};
  }

  return CalibrationColumnFeatures{encoding_type_to_string.left.at(EncodingType::Unencoded), false,
                                   data_type_to_string.left.at(column_expression->data_type()),
                                   _get_memory_usage_for_column(left_input_table, column_id)};
}

void CostModelFeatureExtractor::_extract_table_scan_features_for_predicate_expression(
    std::shared_ptr<const Table>& left_input_table, CalibrationTableScanFeatures& features,
    const std::shared_ptr<AbstractPredicateExpression>& expression) {
  features.scan_operator_type = predicate_condition_to_string.left.at(expression->predicate_condition);

  const auto& predicate_arguments = expression->arguments;

  // TODO(Sven): for now, only column expressions are evaluated as they are expected to be expensive

  // TODO(Sven): This expects a binary or between expression
  if (predicate_arguments.size() == 2 || predicate_arguments.size() == 3) {
    const auto& first_argument = predicate_arguments[0];
    if (first_argument->type == ExpressionType::PQPColumn) {
      const auto& column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(first_argument);
      features.first_column = _extract_features_for_column_expression(left_input_table, column_expression);
    }

    const auto& second_argument = predicate_arguments[1];
    if (second_argument->type == ExpressionType::PQPColumn) {
      features.is_column_comparison = true;

      const auto& column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(second_argument);
      features.second_column = _extract_features_for_column_expression(left_input_table, column_expression);
    }

    if (predicate_arguments.size() == 3) {
      const auto& third_argument = predicate_arguments[2];
      if (third_argument->type == ExpressionType::PQPColumn) {
        const auto& column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(third_argument);
        features.third_column = _extract_features_for_column_expression(left_input_table, column_expression);
      }
    }

  } else {
    std::cout << "facing unexpected table scan with 1 or more than 3 predicates: " << *expression << std::endl;
  }
}

size_t CostModelFeatureExtractor::_get_memory_usage_for_column(const std::shared_ptr<const Table>& table,
                                                               ColumnID column_id) {
  size_t memory_usage = 0;

  for (const auto& chunk : table->chunks()) {
    const auto& segment = chunk->get_segment(column_id);
    memory_usage += segment->estimate_memory_usage();
  }

  return memory_usage;
}

std::pair<EncodingType, bool> CostModelFeatureExtractor::_get_encoding_type_for_segment(
    const std::shared_ptr<BaseSegment>& segment) {
  auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);

  // Dereference ReferenceSegment for encoding feature
  // TODO(Sven): add test for empty referenced table
  // TODO(Sven): add test to check for encoded, referenced column
  if (reference_segment && reference_segment->referenced_table()->chunk_count() > ChunkID{0}) {
    auto underlying_segment = reference_segment->referenced_table()
                                  ->get_chunk(ChunkID{0})
                                  ->get_segment(reference_segment->referenced_column_id());
    auto encoded_scan_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(underlying_segment);
    if (encoded_scan_segment) {
      return std::make_pair(encoded_scan_segment->encoding_type(), true);
    }
    return std::make_pair(EncodingType::Unencoded, true);
  } else {
    auto encoded_scan_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
    if (encoded_scan_segment) {
      return std::make_pair(encoded_scan_segment->encoding_type(), false);
    }
    return std::make_pair(EncodingType::Unencoded, false);
  }
}

const std::optional<CalibrationProjectionFeatures> CostModelFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const Projection>& op) {
  CalibrationProjectionFeatures operator_result{};

  // TODO(Sven): Add features that signal whether subselects need to be executed

  // Feature Column Counts
  auto num_input_columns = op->input_table_left()->column_count();
  auto num_output_columns = op->get_output()->column_count();

  operator_result.input_column_count = num_input_columns;
  operator_result.output_column_count = num_output_columns;

  return operator_result;
}

const std::optional<CalibrationJoinFeatures> CostModelFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const AbstractJoinOperator>& op) {
  CalibrationJoinFeatures operator_result{};

  const auto& left_table = op->input_table_left();
  const auto& right_table = op->input_table_right();

  const auto& column_ids = op->column_ids();

  const auto& left_column_expression = PQPColumnExpression::from_table(*left_table, column_ids.first);
  const auto& right_column_expression = PQPColumnExpression::from_table(*left_table, column_ids.second);

//  operator_result.join_type = op->type();
  operator_result.left_join_column = _extract_features_for_column_expression(left_table, left_column_expression);
  operator_result.right_join_column = _extract_features_for_column_expression(right_table, right_column_expression);

  return operator_result;
}

const std::optional<CalibrationAggregateFeatures> CostModelFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const Aggregate>& op) {
  CalibrationAggregateFeatures operator_result{};

  // TODO(Sven): Add some specific features

  return operator_result;
}

}  // namespace opossum
