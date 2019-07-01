#include "column_features.hpp"

#include "constant_mappings.hpp"

namespace opossum {
namespace cost_model {

ColumnFeatures::ColumnFeatures(const std::string& prefix) : _prefix(prefix) {}

const std::map<std::string, AllTypeVariant> ColumnFeatures::serialize() const {
  //  DebugAssert(column_data_type, "Data Type not set in ColumnFeatures");
  const auto data_type_string = column_data_type ? data_type_to_string.left.at(*column_data_type) : "";

  const auto vector_compression_string = (column_segment_vector_compression) ? vector_compression_type_to_string.left.at(*column_segment_vector_compression) : "None";


  return {
      {_prefix + "_column_segment_encoding", pmr_string(encoding_type_to_string.left.at(column_segment_encoding))},
      {_prefix + "_column_segment_vector_compression", pmr_string(vector_compression_string)},
      {_prefix + "_column_is_reference_segment", column_is_reference_segment},
      {_prefix + "_column_data_type", pmr_string(data_type_string)},
      {_prefix + "_column_memory_usage_bytes", static_cast<int64_t>(column_memory_usage_bytes)},
      {_prefix + "_column_distinct_value_count", static_cast<int64_t>(column_distinct_value_count)},
  };
}

const std::unordered_map<std::string, float> ColumnFeatures::to_cost_model_features() const {
  // One-Hot Encoding for Data Type
  std::unordered_map<std::string, float> one_hot_encoded_data_types{};
  for (const auto& [data_type, data_type_string] : data_type_to_string.left) {
    const auto value = (column_data_type == data_type) ? 1.0f : 0.0f;
    const auto feature_name = _prefix + "_column_data_type_" + data_type_string;
    one_hot_encoded_data_types[feature_name] = value;
  }
  one_hot_encoded_data_types[_prefix + "_column_data_type_undefined"] = column_data_type ? 0.0f : 1.0f;

  std::unordered_map<std::string, float> features = {
      {_prefix + "_column_segment_encoding", static_cast<float>(column_segment_encoding)},
      {_prefix + "_column_segment_vector_compression", (column_segment_vector_compression) ? static_cast<float>(*column_segment_vector_compression) : static_cast<float>(255)},
      {_prefix + "_column_is_reference_segment", static_cast<float>(column_is_reference_segment)},
      {_prefix + "_column_memory_usage_bytes", static_cast<float>(column_memory_usage_bytes)},
      {_prefix + "_column_distinct_value_count", static_cast<float>(column_distinct_value_count)},
  };

  features.insert(one_hot_encoded_data_types.begin(), one_hot_encoded_data_types.end());

  return features;
}

}  // namespace cost_model
}  // namespace opossum