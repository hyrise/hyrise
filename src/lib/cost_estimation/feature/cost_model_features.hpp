#pragma once

#include <vector>

#include "abstract_features.hpp"
#include "aggregate_features.hpp"
#include "all_type_variant.hpp"
#include "constant_hardware_features.hpp"
#include "join_features.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "operators/abstract_operator.hpp"
#include "projection_features.hpp"
#include "runtime_hardware_features.hpp"
#include "table_scan_features.hpp"

namespace opossum {
namespace cost_model {

struct CostModelFeatures : public AbstractFeatures {
 public:
  const std::map<std::string, AllTypeVariant> serialize() const override;
  const std::unordered_map<std::string, float> to_cost_model_features() const override;

  OperatorType operator_type;
  uint64_t execution_time_ns = 0;
  float input_table_size_ratio = 0.0;
  size_t total_row_count = 0;
  float logical_cost_sort_merge = 0;
  // DISABLED UNTIL NECESSARY FOR JOIN float logical_cost_hash = 0;

  size_t left_input_row_count = 0;
  // `left_input_data_table_row_count` is used only for scans right now. Purpose is to
  // gather the ratio of tuples of the data table relative to the positions in the PosList.
  size_t left_input_data_table_row_count = 0;
  size_t left_input_chunk_count = 0;
  size_t left_input_memory_usage_bytes = 0;
  size_t left_input_chunk_size = 0;

  size_t right_input_row_count = 0;
  size_t right_input_chunk_count = 0;
  size_t right_input_memory_usage_bytes = 0;
  size_t right_input_chunk_size = 0;

  size_t output_row_count = 0;
  size_t output_chunk_count = 0;
  size_t output_memory_usage_bytes = 0;
  size_t output_chunk_size = 0;

  float selectivity = 0.0f;

  pmr_string operator_description;
  pmr_string previous_operator = "Unknown";


  TableScanFeatures table_scan_features{};
  JoinFeatures join_features{};
};

}  // namespace cost_model
}  // namespace opossum
