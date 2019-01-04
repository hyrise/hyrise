#include "feature/column_features.hpp"

namespace opossum {
namespace cost_model {

ColumnFeatures::ColumnFeatures(const std::string& prefix) : _prefix(prefix) {}

const std::map<std::string, AllTypeVariant> ColumnFeatures::serialize() const {return {
    {_prefix + "_column_segment_encoding_Unencoded_percentage", column_segment_encoding_Unencoded_percentage},
    {_prefix + "_column_segment_encoding_Dictionary_percentage", column_segment_encoding_Dictionary_percentage},
    {_prefix + "_column_segment_encoding_RunLength_percentage", column_segment_encoding_RunLength_percentage},
    {_prefix + "_column_segment_encoding_FixedStringDictionary_percentage",
     column_segment_encoding_FixedStringDictionary_percentage},
    {_prefix + "_column_segment_encoding_FrameOfReference_percentage",
     column_segment_encoding_FrameOfReference_percentage},
    {_prefix + "_column_is_any_segment_reference_segment", column_is_any_segment_reference_segment},
    {_prefix + "_column_data_type", column_data_type},
    {_prefix + "_column_memory_usage_bytes", static_cast<int32_t>(column_memory_usage_bytes)},
    {_prefix + "_column_distinct_value_count", static_cast<int32_t>(column_distinct_value_count)},
}};

}  // namespace cost_model
}  // namespace opossum