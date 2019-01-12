#include "column_feature_extractor.hpp"

#include "expression/expression_utils.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/base_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {
namespace cost_model {

const ColumnFeatures ColumnFeatureExtractor::extract_features(
    const std::shared_ptr<AbstractLQPNode>& node, const std::shared_ptr<LQPColumnExpression>& column_expression,
    const std::string& prefix) {
  const auto column_reference = column_expression->column_reference;
  const auto original_node = column_reference.original_node();
  const auto column_id = column_reference.original_column_id();

  Assert(original_node->type == LQPNodeType::StoredTable, "Can only extract column features for StoredTable");
  const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
  const auto& underlying_table_name = stored_table_node->table_name;

  const auto table = StorageManager::get().get_table(underlying_table_name);
  auto chunk_count = table->chunk_count();

  if (chunk_count == ChunkID{0}) {
    return ColumnFeatures{prefix};
  }

  size_t number_of_reference_segments = 0;
  std::map<EncodingType, size_t> encoding_mapping{{EncodingType::Unencoded, 0},
                                                  {EncodingType::Dictionary, 0},
                                                  {EncodingType::FixedStringDictionary, 0},
                                                  {EncodingType::FrameOfReference, 0},
                                                  {EncodingType::RunLength, 0}};

  for (ChunkID chunk_id{0u}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    const auto segment = chunk->get_segment(column_id);
    const auto encoding_reference_pair = _get_encoding_type_for_segment(segment);

    encoding_mapping[encoding_reference_pair.first] += 1;
    if (encoding_reference_pair.second) {
      number_of_reference_segments++;
    }
  }

  ColumnFeatures column_features{prefix};

  column_features.column_segment_encoding_Unencoded_percentage =
      encoding_mapping[EncodingType::Unencoded] / static_cast<float>(chunk_count);
  column_features.column_segment_encoding_Dictionary_percentage =
      encoding_mapping[EncodingType::Dictionary] / static_cast<float>(chunk_count);
  column_features.column_segment_encoding_RunLength_percentage =
      encoding_mapping[EncodingType::RunLength] / static_cast<float>(chunk_count);
  column_features.column_segment_encoding_FixedStringDictionary_percentage =
      encoding_mapping[EncodingType::FixedStringDictionary] / static_cast<float>(chunk_count);
  column_features.column_segment_encoding_FrameOfReference_percentage =
      encoding_mapping[EncodingType::FrameOfReference] / static_cast<float>(chunk_count);
  column_features.column_reference_segment_percentage = number_of_reference_segments / static_cast<float>(chunk_count);
  column_features.column_data_type = column_expression->data_type();
  // TODO(Sven): this returns the size of the original, stored, unfiltered column...
  column_features.column_memory_usage_bytes = _get_memory_usage_for_column(table, column_id);
  // TODO(Sven): How to calculate from segment_distinct_value_count?
  column_features.column_distinct_value_count = 0;

  return column_features;
}

std::pair<EncodingType, bool> ColumnFeatureExtractor::_get_encoding_type_for_segment(const std::shared_ptr<BaseSegment>& segment) {
  const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);

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

size_t ColumnFeatureExtractor::_get_memory_usage_for_column(const std::shared_ptr<const Table>& table,
                                                            ColumnID column_id) {
  size_t memory_usage = 0;

  for (const auto& chunk : table->chunks()) {
    const auto& segment = chunk->get_segment(column_id);
    memory_usage += segment->estimate_memory_usage();
  }

  return memory_usage;
}

}  // namespace cost_model
}  // namespace opossum
