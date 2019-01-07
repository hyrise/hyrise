#pragma once

#include <vector>

#include "all_type_variant.hpp"
#include "abstract_features.hpp"
#include "aggregate_features.hpp"
#include "constant_hardware_features.hpp"
#include "join_features.hpp"
#include "projection_features.hpp"
#include "runtime_hardware_features.hpp"
#include "table_scan_features.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {
    namespace cost_model {

struct CalibrationFeatures : public AbstractFeatures {
 public:
  const std::map<std::string, AllTypeVariant> serialize() const override;

  OperatorType operator_type;
  uint64_t execution_time_ns = 0;
  float input_table_size_ratio = 0.0;

  size_t left_input_row_count = 0;
  size_t left_input_chunk_count = 0;
  size_t left_input_memory_usage_bytes = 0;
  size_t left_input_chunk_size = 0;
  bool left_input_is_small_table = false;

  size_t right_input_row_count = 0;
  size_t right_input_chunk_count = 0;
  size_t right_input_memory_usage_bytes = 0;
  size_t right_input_chunk_size = 0;
  bool right_input_is_small_table = false;

  size_t output_row_count = 0;
  size_t output_chunk_count = 0;
  size_t output_memory_usage_bytes = 0;
  size_t output_chunk_size = 0;
  bool output_is_small_table = false;

  float selectivity = 0.0;
  bool is_selectivity_below_50_percent = false;
  float selectivity_distance_to_50_percent = false;

  // Just for debugging
  std::string operator_description;

  ConstantHardwareFeatures constant_hardware_features;
  RuntimeHardwareFeatures runtime_hardware_features;

  AggregateFeatures aggregate_features {};
  JoinFeatures join_features {};
  ProjectionFeatures projection_features {};
  TableScanFeatures table_scan_features {};
};

}  // namespace cost_model
}  // namespace opossum
