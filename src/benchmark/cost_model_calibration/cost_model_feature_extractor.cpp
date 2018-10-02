#include "cost_model_feature_extractor.hpp"

#include <json.hpp>

#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/reference_segment.hpp"

namespace opossum {

const nlohmann::json CostModelFeatureExtractor::extract_features(const std::shared_ptr<const AbstractOperator>& op) {
  nlohmann::json operator_result{};

  auto time = op->performance_data().walltime;
  auto execution_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time).count();

  operator_result["execution_time_ns"] = execution_time_ns;

  if (const auto& output = op->get_output()) {
    // Inputs
    auto left_input = (op->input_left()) ? op->input_left()->get_output() : nullptr;
    auto right_input = (op->input_right()) ? op->input_right()->get_output() : nullptr;

    auto left_input_row_count = (left_input) ? left_input->row_count() : 0;
    auto left_input_chunk_count = (left_input) ? left_input->chunk_count() : 0;
    auto left_input_memory_usage = (left_input) ? left_input->estimate_memory_usage() : 0;
    auto left_input_chunk_size = (left_input) ? left_input->max_chunk_size() : 0;

    auto right_input_row_count = (right_input) ? right_input->row_count() : 0;
    auto right_input_chunk_count = (right_input) ? right_input->chunk_count() : 0;
    auto right_input_memory_usage = (right_input) ? right_input->estimate_memory_usage() : 0;
    auto right_input_chunk_size = (right_input) ? right_input->max_chunk_size() : 0;

    if (right_input_row_count != 0) {
      if (left_input_row_count > right_input_row_count) {
        operator_result["input_table_size_ratio"] = left_input_row_count / right_input_row_count;
      } else {
        operator_result["input_table_size_ratio"] = right_input_row_count / left_input_row_count;
      }
    }

    operator_result["left_input_row_count"] = left_input_row_count;
    operator_result["left_input_chunk_count"] = left_input_chunk_count;
    operator_result["left_input_memory_usage_bytes"] = left_input_memory_usage;
    operator_result["left_input_chunk_size"] = left_input_chunk_size;

    operator_result["right_input_row_count"] = right_input_row_count;
    operator_result["right_input_chunk_count"] = right_input_chunk_count;
    operator_result["right_input_memory_usage_bytes"] = right_input_memory_usage;
    operator_result["right_input_chunk_size"] = right_input_chunk_size;

    // Output
    auto output_row_count = output->row_count();
    // Calculate cross-join cardinality.
    // Use 1 for cases, in which one side is empty to avoid divisions by zero in the next step
    auto total_input_row_count =
        std::max<uint64_t>(1, left_input_row_count) * std::max<uint64_t>(1, right_input_row_count);
    auto output_selectivity = output_row_count / static_cast<double>(total_input_row_count);
    auto output_chunk_count = output->chunk_count();
    auto output_memory_usage = output->estimate_memory_usage();
    auto output_chunk_size = output->max_chunk_size();

    operator_result["output_row_count"] = output_row_count;
    operator_result["output_chunk_count"] = output_chunk_count.t;
    operator_result["output_memory_usage_bytes"] = output_memory_usage;
    operator_result["output_chunk_size"] = output_chunk_size;
    operator_result["output_selectivity"] = output_selectivity;

    auto hardware_features = _extract_constant_hardware_features();
    operator_result.insert(hardware_features.begin(), hardware_features.end());

    auto runtime_features = _extract_runtime_hardware_features();
    operator_result.insert(runtime_features.begin(), runtime_features.end());

    auto description = op->name();
    if (description == "TableScan") {
      auto table_scan_op = std::static_pointer_cast<const TableScan>(op);
      auto table_scan_result = _extract_features_for_operator(table_scan_op);

      if (table_scan_result.empty()) {
        return operator_result;
      }

      operator_result.insert(table_scan_result.begin(), table_scan_result.end());
    } else if (description == "Projection") {
      auto projection_op = std::static_pointer_cast<const Projection>(op);
      auto projection_result = _extract_features_for_operator(projection_op);

      if (projection_result.empty()) {
        return operator_result;
      }

      operator_result.insert(projection_result.begin(), projection_result.end());
    } else if (description == "JoinHash") {
      auto join_hash_op = std::static_pointer_cast<const JoinHash>(op);
      auto join_hash_result = _extract_features_for_operator(join_hash_op);

      if (join_hash_result.empty()) {
        return operator_result;
      }

      operator_result.insert(join_hash_result.begin(), join_hash_result.end());
    }
  }
  return operator_result;
}

const nlohmann::json CostModelFeatureExtractor::_extract_constant_hardware_features() {
  nlohmann::json hardware_features{};

  // Hard-coded to MacBook Config
  hardware_features["l1_size_kb"] = 0;
  hardware_features["l1_block_size_kb"] = 0;
  hardware_features["l2_size_kb"] = 256;
  hardware_features["l2_block_size_kb"] = 0;
  hardware_features["l3_size_mb"] = 3;
  hardware_features["l3_block_size_mb"] = 0;

  hardware_features["memory_size_gb"] = 8;
  hardware_features["memory_access_bandwith"] = 0;
  hardware_features["memory_access_latency"] = 0;
  hardware_features["cpu_architecture"] = 0;  // Should be ENUM
  hardware_features["num_cpu_cores"] = 2;
  hardware_features["cpu_clock_speed_mhz"] = 2400;
  hardware_features["num_numa_nodes"] = 0;

  return hardware_features;
}

const nlohmann::json CostModelFeatureExtractor::_extract_runtime_hardware_features() {
  nlohmann::json runtime_features{};

  // Current System Load
  runtime_features["current_memory_consumption_percentage"] = 0;
  runtime_features["running_queries"] = 0;
  runtime_features["remaining_transactions"] = 0;

  return runtime_features;
}

const nlohmann::json CostModelFeatureExtractor::_extract_features_for_operator(
    const std::shared_ptr<const TableScan>& op) {
  nlohmann::json operator_result{};

  auto left_input_table = op->input_table_left();
  auto table_scan_op = std::static_pointer_cast<const TableScan>(op);
  auto chunk_count = left_input_table->chunk_count();

  if (chunk_count <= ChunkID{0}) {
    return operator_result;
  }

  auto scan_column = left_input_table->get_chunk(ChunkID{0})->get_segment(table_scan_op->predicate().column_id);

  auto scan_column_data_type = scan_column->data_type();
  auto scan_column_memory_usage_bytes = scan_column->estimate_memory_usage();

  auto reference_column = std::dynamic_pointer_cast<ReferenceSegment>(scan_column);
  operator_result["is_scan_column_reference_column"] = reference_column ? true : false;

  // Dereference ReferenceColumn for encoding feature
  if (reference_column && reference_column->referenced_table()->chunk_count() > ChunkID{0}) {
    auto underlying_column = reference_column->referenced_table()
                                 ->get_chunk(ChunkID{0})
                                 ->get_segment(reference_column->referenced_column_id());
    auto encoded_scan_column = std::dynamic_pointer_cast<const BaseEncodedSegment>(underlying_column);
    if (encoded_scan_column) {
      operator_result["scan_column_encoding"] = encoded_scan_column->encoding_type();
    } else {
      operator_result["scan_column_encoding"] = EncodingType::Unencoded;
    }
  } else {
    auto encoded_scan_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(scan_column);
    if (encoded_scan_segment) {
      operator_result["scan_column_encoding"] = encoded_scan_segment->encoding_type();
    } else {
      operator_result["scan_column_encoding"] = EncodingType::Unencoded;
    }
  }

  operator_result["scan_column_data_type"] = scan_column_data_type;
  operator_result["scan_column_memory_usage_bytes"] = scan_column_memory_usage_bytes;
  operator_result["scan_column_distinct_value_count"] = 0;  // TODO(Sven): Ask Statistics for detailed information.

  return operator_result;
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
