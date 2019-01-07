#include "column_features.hpp"

#include "constant_mappings.hpp"

namespace opossum {
namespace cost_model {

ColumnFeatures::ColumnFeatures(const std::string& prefix) : _prefix(prefix) {}

const std::map<std::string, AllTypeVariant> ColumnFeatures::serialize() const {
  //  DebugAssert(column_data_type, "Data Type not set in ColumnFeatures");
  const auto data_type_string = column_data_type ? data_type_to_string.left.at(*column_data_type) : "";

  return {
      {_prefix + "_column_segment_encoding_Unencoded_percentage", column_segment_encoding_Unencoded_percentage},
      {_prefix + "_column_segment_encoding_Dictionary_percentage", column_segment_encoding_Dictionary_percentage},
      {_prefix + "_column_segment_encoding_RunLength_percentage", column_segment_encoding_RunLength_percentage},
      {_prefix + "_column_segment_encoding_FixedStringDictionary_percentage",
       column_segment_encoding_FixedStringDictionary_percentage},
      {_prefix + "_column_segment_encoding_FrameOfReference_percentage",
       column_segment_encoding_FrameOfReference_percentage},
      {_prefix + "_column_reference_segment_percentage", column_reference_segment_percentage},
      {_prefix + "_column_data_type", data_type_string},
      {_prefix + "_column_memory_usage_bytes", static_cast<int64_t>(column_memory_usage_bytes)},
      {_prefix + "_column_distinct_value_count", static_cast<int64_t>(column_distinct_value_count)},
  };
}

}  // namespace cost_model
}  // namespace opossum