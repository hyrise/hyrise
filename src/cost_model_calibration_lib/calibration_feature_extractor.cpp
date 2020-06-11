#include "calibration_feature_extractor.hpp"

#include "constant_mappings.hpp"
#include "cost_estimation/feature_extractor/column_feature_extractor.hpp"
#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/index_scan.hpp"
#include "storage/abstract_encoded_segment.hpp"
#include "storage/reference_segment.hpp"

namespace opossum {
namespace cost_model {

const std::optional<CostModelFeatures> CalibrationFeatureExtractor::extract_features(
    const std::shared_ptr<const AbstractOperator>& op) {
  auto calibration_result = _extract_general_features(op);

  auto operator_type = op->type();

  //  // TODO(Sven): add test
  switch (operator_type) {
    case OperatorType::TableScan: {
      const auto table_scan_op = std::static_pointer_cast<const TableScan>(op);

      const auto table_scan_features = _extract_features_for_operator(table_scan_op);
      if (table_scan_features)
        calibration_result.table_scan_features = *table_scan_features;
      else
        return std::nullopt;
      break;
    }
    case OperatorType::IndexScan: {
      const auto index_scan_op = std::static_pointer_cast<const IndexScan>(op);
      calibration_result.table_scan_features = _extract_features_for_operator(index_scan_op);
      break;
    }
    case OperatorType::Validate: {
      const auto validate_op = std::static_pointer_cast<const Validate>(op);

      const auto table_scan_features = _extract_features_for_operator(validate_op);
      if (table_scan_features)
        calibration_result.table_scan_features = *table_scan_features;
      else
        return std::nullopt;
      break;
    }
    case OperatorType::GetTable:
      // No need to add specific features
      break;
    default: {
      std::cout << "Unhandled operator type in CalibrationFeatureExtractor." << std::endl;
    }
  }

  return calibration_result;
}

const CostModelFeatures CalibrationFeatureExtractor::_extract_general_features(
    const std::shared_ptr<const AbstractOperator>& op) {
  CostModelFeatures operator_features{};
  const auto time = op->performance_data->walltime;
  const auto execution_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time).count();

  operator_features.execution_time_ns = execution_time_ns;
  operator_features.operator_type = op->type();
  operator_features.operator_description = op->description(DescriptionMode::SingleLine);

  // Inputs
  if (op->left_input()) {
    auto left_parent_op = op->left_input();

    const auto max_iterations = size_t{1'000};
    auto iterations = size_t{0};
    operator_features.previous_operator = left_parent_op->name();
    while (left_parent_op->type() != OperatorType::GetTable && iterations < max_iterations) {
      // Get left inputs until we find a GetTable operator.
      // TODO(anyone): ensure we will always end up at a GetTable have covered all corner cases.
      left_parent_op = left_parent_op->left_input();
      ++iterations;
    } 
    if (left_parent_op->type() == OperatorType::GetTable) {
      // If we found a GetTable node, get the size of the table.
      const auto get_table_op = std::static_pointer_cast<const GetTable>(left_parent_op);
      operator_features.left_input_data_table_row_count = op->performance_data->output_row_count;
    }

    const auto left_input = op->left_input()->get_output();
    operator_features.left_input_row_count = op->left_input()->performance_data->output_row_count;
    operator_features.left_input_chunk_count = op->left_input()->performance_data->output_chunk_count;
    operator_features.left_input_memory_usage_bytes = -1;
    operator_features.left_input_chunk_size = -1;
  }

  if (op->right_input()) {
    const auto right_input = op->right_input()->get_output();
    operator_features.right_input_row_count = op->right_input()->performance_data->output_row_count;
    operator_features.right_input_chunk_count = op->right_input()->performance_data->output_chunk_count;
    operator_features.right_input_memory_usage_bytes = -1;
    operator_features.right_input_chunk_size = -1;
  }

  const auto left_input_row_count = operator_features.left_input_row_count;
  const auto right_input_row_count = operator_features.right_input_row_count;

  // Output
  if (const auto& output = op->get_output()) {
    // Output selectivity seems to be off
    const auto output_row_count = op->performance_data->output_row_count;
    // Calculate cross-join cardinality.
    // Use 1 for cases, in which one side is empty to avoid divisions by zero in the next step
    const auto total_input_row_count =
        std::max<uint64_t>(1, left_input_row_count) * std::max<uint64_t>(1, right_input_row_count);
    const auto output_selectivity = std::min<float>(1.0f, static_cast<float>(output_row_count) / static_cast<float>(total_input_row_count));

    operator_features.selectivity = output_selectivity;
    operator_features.output_row_count = output_row_count;
    operator_features.output_chunk_count = op->performance_data->output_chunk_count;
    operator_features.output_memory_usage_bytes = output->memory_usage(MemoryUsageCalculationMode::Full);

    const auto output_chunk_size = output->target_chunk_size();
    operator_features.output_chunk_size = output_chunk_size;
  }

  return operator_features;
}

const ConstantHardwareFeatures CalibrationFeatureExtractor::_extract_constant_hardware_features() { return {}; }

const RuntimeHardwareFeatures CalibrationFeatureExtractor::_extract_runtime_hardware_features() { return {}; }

const std::optional<TableScanFeatures> CalibrationFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const TableScan>& op) {
  TableScanFeatures features{};

  const auto lqp = op->lqp_node;

  const auto row_count = op->left_input()->performance_data->output_row_count;
  const auto chunk_count = op->left_input()->performance_data->output_chunk_count;

  if (row_count == 0)
    return std::nullopt;

  const auto& table_condition = op->predicate();
  features.computable_or_column_expression_count = count_expensive_child_expressions(table_condition);
  features.effective_chunk_count = chunk_count - op->excluded_chunk_ids.size();

  if (features.effective_chunk_count == 0)
    return std::nullopt;

  if (table_condition->type == ExpressionType::Predicate) {
    const auto& casted_predicate = std::dynamic_pointer_cast<AbstractPredicateExpression>(table_condition);
    _extract_table_scan_features_for_predicate_expression(left_input_table, features, casted_predicate);
  } else if (table_condition->type == ExpressionType::Logical) {
    const auto logical_expression = std::dynamic_pointer_cast<LogicalExpression>(table_condition);
    if (logical_expression->logical_operator == LogicalOperator::Or) {
      const auto& casted_predicate = std::dynamic_pointer_cast<LogicalExpression>(table_condition);
      std::ostringstream lqp_stream;
      lqp_stream << casted_predicate->logical_operator;
      features.scan_operator_type = lqp_stream.str();
      //          const auto& predicate_arguments = casted_predicate->arguments;
    }
  }

  return features;
}

const TableScanFeatures CalibrationFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const IndexScan>& op) {
  TableScanFeatures features{};

  auto left_input_table = op->left_input_table();
  const auto left_column_ids = op->left_columns_ids();
  const auto& predicate_condition = op->predicate_condition();
  const auto predicate_condition_pointer = std::make_shared<PredicateCondition>(predicate_condition);

  features.scan_operator_type = predicate_condition_to_string.left.at(predicate_condition);
  features.effective_chunk_count = op->included_chunk_ids.size();

  DebugAssert(left_column_ids.size() == 1, "Expected only one column for IndexScan in FeatureExtractor");
  const auto column_expression = PQPColumnExpression::from_table(*left_input_table, left_column_ids.front());
  features.first_column = _extract_features_for_column_expression(left_input_table, column_expression, "first");

  return features;
}

const std::optional<TableScanFeatures> CalibrationFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const Validate>& op) {
  TableScanFeatures features{};

  const auto left_input = op->left_input();
  if (left_input->performance_data->output_row_count == 0 || left_input->performance_data->output_chunk_count == 0)
    return std::nullopt;

  features.effective_chunk_count = left_input->performance_data->output_chunk_count;
  return features;
}

const ColumnFeatures CalibrationFeatureExtractor::_extract_features_for_column_expression(
    const std::shared_ptr<const Table>& left_input_table, const std::shared_ptr<PQPColumnExpression>& column_expression,
    const std::string& prefix) {
  const auto column_id = column_expression->column_id;

  return ColumnFeatureExtractor::extract_features(left_input_table, column_id, column_expression->data_type(), prefix);
}

void CalibrationFeatureExtractor::_extract_table_scan_features_for_predicate_expression(
    std::shared_ptr<const Table>& left_input_table, TableScanFeatures& features,
    const std::shared_ptr<AbstractPredicateExpression>& expression) {
  features.scan_operator_type = predicate_condition_to_string.left.at(expression->predicate_condition);

  const auto& predicate_arguments = expression->arguments;

  // TODO(Sven): for now, only column expressions are evaluated as they are expected to be expensive

  // TODO(Sven): This expects a binary or between expression
  if (predicate_arguments.size() == 2 || predicate_arguments.size() == 3) {
    const auto& first_argument = predicate_arguments[0];
    if (first_argument->type == ExpressionType::PQPColumn) {
      const auto& column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(first_argument);
      features.first_column = _extract_features_for_column_expression(left_input_table, column_expression, "first");
    }

    const auto& second_argument = predicate_arguments[1];
    if (second_argument->type == ExpressionType::PQPColumn) {
      features.is_column_comparison = true;

      const auto& column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(second_argument);
      features.second_column = _extract_features_for_column_expression(left_input_table, column_expression, "second");
    }

    // if (predicate_arguments.size() == 3) {
    //   const auto& third_argument = predicate_arguments[2];
    //   if (third_argument->type == ExpressionType::PQPColumn) {
    //     const auto& column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(third_argument);
    //     features.third_column = _extract_features_for_column_expression(left_input_table, column_expression, "third");
    //   }
    // }

  } else {
    std::cout << "facing unexpected table scan with 1 or more than 3 predicates: " << *expression << std::endl;
  }
}

size_t CalibrationFeatureExtractor::_get_memory_usage_for_column(const std::shared_ptr<const Table>& table,
                                                                 const ColumnID column_id) {
  size_t memory_usage = 0;

  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    const auto& segment = chunk->get_segment(column_id);
    memory_usage += segment->memory_usage(MemoryUsageCalculationMode::Full);
  }

  return memory_usage;
}

const ProjectionFeatures CalibrationFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const Projection>& op) {
  ProjectionFeatures features{};
  // TODO(Sven): Add features that signal whether subselects need to be executed
  features.input_column_count = op->left_input_table()->column_count();
  features.output_column_count = op->get_output()->column_count();

  return features;
}

const JoinFeatures CalibrationFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const AbstractJoinOperator>& op) {
  JoinFeatures features{};
  const auto& left_table = op->left_input_table();
  const auto& right_table = op->right_input_table();

  const auto& column_ids = op->primary_predicate().column_ids;

  const auto& left_column_expression = PQPColumnExpression::from_table(*left_table, column_ids.first);
  const auto& right_column_expression = PQPColumnExpression::from_table(*right_table, column_ids.second);

  // TODO(Sven): Remove. Is already covered by features.operator_type
  //  features.join_type = op->type();
  features.left_join_column = _extract_features_for_column_expression(left_table, left_column_expression, "left");
  features.right_join_column = _extract_features_for_column_expression(right_table, right_column_expression, "right");

  return features;
}

const AggregateFeatures CalibrationFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const AbstractAggregateOperator>& op) {
  return {};
}

}  // namespace cost_model
}  // namespace opossum
