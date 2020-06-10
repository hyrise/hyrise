#include "column_feature_extractor.hpp"

#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "resolve_type.hpp"
#include "statistics/abstract_attribute_statistics.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/base_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/storage_manager.hpp"
#include "storage/vector_compression/vector_compression.hpp"

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

  const auto table = Hyrise::get().storage_manager.get_table(underlying_table_name);

  return extract_features(table, column_id, column_expression->data_type(), prefix);
}

const ColumnFeatures ColumnFeatureExtractor::extract_features(const std::shared_ptr<const Table>& table,
                                                              const ColumnID column_id, const DataType data_type,
                                                              const std::string& prefix) {
  auto chunk_count = table->chunk_count();
  std::map<EncodingType, size_t> encoding_mapping{{EncodingType::Unencoded, 0},
                                                  {EncodingType::Dictionary, 0},
                                                  {EncodingType::FixedStringDictionary, 0},
                                                  {EncodingType::FrameOfReference, 0},
                                                  {EncodingType::RunLength, 0},
                                                  {EncodingType::LZ4, 0}};

  std::map<VectorCompressionType, size_t> vector_compression_mapping{{VectorCompressionType::FixedSizeByteAligned, 0},
                                                                     {VectorCompressionType::SimdBp128, 0}};

  EncodingType table_encoding_type = EncodingType::Unencoded;
  std::optional<VectorCompressionType> vector_compression_type;
  for (ChunkID chunk_id{0u}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    const auto segment = chunk->get_segment(column_id);
    const auto encoding_reference_pair = _get_encoding_type_for_segment(segment);

    const auto segment_encoding_spec = encoding_reference_pair.first;

    encoding_mapping[segment_encoding_spec.encoding_type] += 1;
    table_encoding_type = segment_encoding_spec.encoding_type;

    if (segment_encoding_spec.vector_compression_type) {
      vector_compression_type = *segment_encoding_spec.vector_compression_type;
      vector_compression_mapping[*segment_encoding_spec.vector_compression_type] += 1;
    }
  }

  Assert(encoding_mapping[table_encoding_type] == static_cast<size_t>(chunk_count), "Tables should not have mixed encodings!");

  if (vector_compression_type)
    Assert(vector_compression_mapping[*vector_compression_type] == static_cast<size_t>(chunk_count), "Tables should not have mixed compression types!");

  ColumnFeatures column_features{prefix};

  column_features.column_segment_encoding = table_encoding_type;
  column_features.column_segment_vector_compression = vector_compression_type;
  column_features.column_is_reference_segment = table->type() == TableType::References;
  column_features.column_data_type = data_type;
  // TODO(Sven): this returns the size of the original, stored, unfiltered column...
  column_features.column_memory_usage_bytes = _get_memory_usage_for_column(table, column_id);

  // We assume that each column has a histogram, which is (as of now) the
  // case since we add all calibration tables to the storage manager.
  if (table->table_statistics() && column_id < table->table_statistics()->column_statistics.size()) {
    const auto base_attribute_statistics = table->table_statistics()->column_statistics[column_id];
    resolve_data_type(data_type, [&](const auto column_data_type) {
      using ColumnDataType = typename decltype(column_data_type)::type;

      const auto attribute_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(base_attribute_statistics);
      if (attribute_statistics) {
        const auto equal_distinct_count_histogram = std::dynamic_pointer_cast<EqualDistinctCountHistogram<ColumnDataType>>(attribute_statistics->histogram);
        if (equal_distinct_count_histogram) {
          column_features.column_distinct_value_count = static_cast<size_t>(equal_distinct_count_histogram->total_distinct_count());
          Assert(column_features.column_distinct_value_count > 0, "Should not happen");
        } else {
          Assert(false, "Should not happen");
        }
      } else {
        Assert(false, "Should not happen");
      }
    });
  } else {
    if (table->type() == TableType::Data) {
      if (!table->table_statistics()) {
        const auto& cn = table->column_names();
        for (const auto& c : cn) {
          std::cout << c << std::endl;
        }
        Assert(false, "Should not happen");
      }
      if (column_id < table->table_statistics()->column_statistics.size()) {
        Assert(false, "Should not happen");
      }
    }
    if (table->type() != TableType::References) {
      Assert(false, "Should not happen");
    }
  }

  return column_features;
}

std::pair<SegmentEncodingSpec, bool> ColumnFeatureExtractor::_get_encoding_type_for_segment(
    const std::shared_ptr<BaseSegment>& segment) {
  const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);

  // Dereference ReferenceSegment for encoding feature
  // TODO(Sven): add test for empty referenced table
  // TODO(Sven): add test to check for encoded, referenced column
  bool is_reference_segment = false;
  auto segment_to_encode = segment;  // initialize with passed segment
  if (reference_segment && reference_segment->referenced_table()->chunk_count() > ChunkID{0}) {
    is_reference_segment = true;
    auto underlying_segment = reference_segment->referenced_table()
                                  ->get_chunk(ChunkID{0})
                                  ->get_segment(reference_segment->referenced_column_id());
    segment_to_encode = underlying_segment;
  }

  auto encoded_scan_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment_to_encode);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
  if (encoded_scan_segment) {
    auto segment_encoding_spec = SegmentEncodingSpec{encoded_scan_segment->encoding_type()};
    if (encoded_scan_segment->compressed_vector_type()) {
      segment_encoding_spec.vector_compression_type =
          parent_vector_compression_type(*encoded_scan_segment->compressed_vector_type());
    }
    return std::make_pair(segment_encoding_spec, is_reference_segment);
  }
#pragma GCC diagnostic pop
  return std::make_pair(SegmentEncodingSpec{EncodingType::Unencoded}, is_reference_segment);
}

size_t ColumnFeatureExtractor::_get_memory_usage_for_column(const std::shared_ptr<const Table>& table,
                                                            const ColumnID column_id) {
  size_t memory_usage = 0;

  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    const auto& segment = chunk->get_segment(column_id);
    memory_usage += segment->memory_usage(MemoryUsageCalculationMode::Full);
  }

  return memory_usage;
}

}  // namespace cost_model
}  // namespace opossum
