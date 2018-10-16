#include "cost_model_feature_extractor.hpp"

#include <json.hpp>

#include <sys/resource.h>

#include "cost_model_calibration/feature/calibration_features.hpp"
#include "cost_model_calibration/feature/calibration_table_scan_features.hpp"
#include "cost_model_calibration/feature/calibration_constant_hardware_features.hpp"
#include "cost_model_calibration/feature/calibration_runtime_hardware_features.hpp"
#include "all_parameter_variant.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/reference_segment.hpp"

namespace opossum {

const nlohmann::json CostModelFeatureExtractor::extract_features(const std::shared_ptr<const AbstractOperator>& op) {
  if (const auto& output = op->get_output()) {
    CalibrationFeatures operator_result{};

    auto time = op->performance_data().walltime;
    auto execution_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time).count();

    operator_result.execution_time_ns = execution_time_ns;
    // Inputs
    if (op->input_left()) {
      const auto left_input = op->input_left()->get_output();
      operator_result.left_input_row_count = left_input->row_count();
      operator_result.left_input_chunk_count = left_input->chunk_count();
      operator_result.left_input_memory_usage_bytes = left_input->estimate_memory_usage();
      operator_result.left_input_chunk_size = left_input->max_chunk_size();
    }

    if (op->input_right()) {
      const auto right_input = op->input_right()->get_output();
      operator_result.right_input_row_count = right_input->row_count();
      operator_result.right_input_chunk_count = right_input->chunk_count();
      operator_result.right_input_memory_usage_bytes = right_input->estimate_memory_usage();
      operator_result.right_input_chunk_size = right_input->max_chunk_size();
    }

    const auto left_input_row_count = operator_result.left_input_row_count;
    const auto right_input_row_count = operator_result.right_input_row_count;

    if (left_input_row_count > 0 && right_input_row_count) {
      if (left_input_row_count > right_input_row_count) {
        operator_result.input_table_size_ratio = left_input_row_count / right_input_row_count;
      } else {
        operator_result.input_table_size_ratio = right_input_row_count / left_input_row_count;
      }
    }

    // Output
    const auto output_row_count = output->row_count();
    // Calculate cross-join cardinality.
    // Use 1 for cases, in which one side is empty to avoid divisions by zero in the next step
    auto total_input_row_count =
        std::max<uint64_t>(1, left_input_row_count) * std::max<uint64_t>(1, right_input_row_count);
    auto output_selectivity = output_row_count / static_cast<double>(total_input_row_count);
    operator_result.output_selectivity = output_selectivity;

    operator_result.output_row_count = output_row_count;
    operator_result.output_chunk_count = output->chunk_count();
    operator_result.output_memory_usage_bytes = output->estimate_memory_usage();
    operator_result.output_chunk_size = output->max_chunk_size();

    nlohmann::json json_operator_result = operator_result;

    auto hardware_features = _extract_constant_hardware_features();
    json_operator_result.insert(hardware_features.begin(), hardware_features.end());

    auto runtime_features = _extract_runtime_hardware_features();
    json_operator_result.insert(runtime_features.begin(), runtime_features.end());

    auto description = op->name();
    if (description == "TableScan") {
      auto table_scan_op = std::static_pointer_cast<const TableScan>(op);
      auto table_scan_result = _extract_features_for_operator(table_scan_op);

      if (table_scan_result.empty()) {
        return json_operator_result;
      }

      json_operator_result.insert(table_scan_result.begin(), table_scan_result.end());
    } else if (description == "Projection") {
      auto projection_op = std::static_pointer_cast<const Projection>(op);
      auto projection_result = _extract_features_for_operator(projection_op);

      if (projection_result.empty()) {
        return json_operator_result;
      }

      json_operator_result.insert(projection_result.begin(), projection_result.end());
    } else if (description == "JoinHash") {
      auto join_hash_op = std::static_pointer_cast<const JoinHash>(op);
      auto join_hash_result = _extract_features_for_operator(join_hash_op);

      if (join_hash_result.empty()) {
        return json_operator_result;
      }

      json_operator_result.insert(join_hash_result.begin(), join_hash_result.end());
    }

    return json_operator_result;
  }
  return {};
}

const nlohmann::json CostModelFeatureExtractor::_extract_constant_hardware_features() {
  CalibrationConstantHardwareFeatures hardware_features{};
  return hardware_features;
}

const nlohmann::json CostModelFeatureExtractor::_extract_runtime_hardware_features() {
  CalibrationRuntimeHardwareFeatures runtime_features{};
  return runtime_features;
}

const nlohmann::json CostModelFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const TableScan>& op) {
  CalibrationTableScanFeatures features {};

  auto left_input_table = op->input_table_left();
  auto table_scan_op = std::static_pointer_cast<const TableScan>(op);
  auto chunk_count = left_input_table->chunk_count();

  if (chunk_count <= ChunkID{0}) {
    return features;
  }

  features.scan_operator_type = table_scan_op->predicate().predicate_condition;

  const auto segment = left_input_table->get_chunk(ChunkID{0})->get_segment(table_scan_op->predicate().column_id);
  features.scan_segment_data_type = segment->data_type();

  auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
  features.is_scan_segment_reference_segment = reference_segment ? true : false;
  features.scan_segment_encoding = _get_encoding_type_for_segment(reference_segment);
  features.scan_segment_memory_usage_bytes = _get_memory_usage_for_column(left_input_table, table_scan_op->predicate().column_id);

  if (is_column_id(table_scan_op->predicate().value)) {
    // Facing table scan with column_id left and right of operator
    const auto second_scan_id = boost::get<ColumnID>(table_scan_op->predicate().value);
    const auto second_scan_segment = left_input_table->get_chunk(ChunkID{0})->get_segment(second_scan_id);
    const auto second_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(second_scan_segment);

    features.uses_second_column = true;

    features.is_second_scan_segment_reference_segment = second_reference_segment ? true : false;
    features.second_scan_segment_encoding = _get_encoding_type_for_segment(second_reference_segment);
    features.second_scan_segment_memory_usage_bytes = _get_memory_usage_for_column(left_input_table, second_scan_id);
    features.second_scan_segment_data_type = second_scan_segment->data_type();
  }

  // Mainly for debugging purposes
  features.scan_operator_description = op->description(DescriptionMode::SingleLine);

  return features;
}

size_t CostModelFeatureExtractor::_get_memory_usage_for_column(const std::shared_ptr<const Table>& table, ColumnID column_id) {
  size_t memory_usage = 0;

  for (const auto & chunk : table->chunks()) {
    const auto & segment = chunk->get_segment(column_id);
    memory_usage += segment->estimate_memory_usage();
  }

  return memory_usage;
}

EncodingType CostModelFeatureExtractor::_get_encoding_type_for_segment(const std::shared_ptr<ReferenceSegment>& reference_segment) {
    // Dereference ReferenceSegment for encoding feature
    if (reference_segment && reference_segment->referenced_table()->chunk_count() > ChunkID{0}) {
        auto underlying_segment = reference_segment->referenced_table()
                ->get_chunk(ChunkID{0})
                ->get_segment(reference_segment->referenced_column_id());
        auto encoded_scan_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(underlying_segment);
        if (encoded_scan_segment) {
            return encoded_scan_segment->encoding_type();
        }
    } else {
        auto encoded_scan_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(reference_segment);
        if (encoded_scan_segment) {
            return encoded_scan_segment->encoding_type();
        }
    }

    return EncodingType::Unencoded;
}

const nlohmann::json CostModelFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const Projection>& op) {
  nlohmann::json operator_result{};

  // TODO(Sven): Add features that signal whether subselects need to be executed

  // Feature Column Counts
  auto num_input_columns = op->input_table_left()->column_count();
  auto num_output_columns = op->get_output()->column_count();

  operator_result["input_column_count"] = num_input_columns;
  operator_result["output_column_count"] = num_output_columns;

  return operator_result;
}
const nlohmann::json CostModelFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const JoinHash>& op) {
  nlohmann::json operator_result{};

  // TODO(Sven): Add some join specific features

  return operator_result;
}

}  // namespace opossum
