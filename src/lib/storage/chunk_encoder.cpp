

#include "base_value_segment.hpp" // NEEDEDINCLUDE
#include "table.hpp" // NEEDEDINCLUDE

#include "statistics/chunk_statistics/chunk_statistics.hpp" // NEEDEDINCLUDE
#include "storage/base_encoded_segment.hpp" // NEEDEDINCLUDE
#include "storage/segment_encoding_utils.hpp" // NEEDEDINCLUDE

namespace opossum {

void ChunkEncoder::encode_chunk(const std::shared_ptr<Chunk>& chunk, const std::vector<DataType>& column_data_types,
                                const ChunkEncodingSpec& chunk_encoding_spec) {
  Assert((column_data_types.size() == chunk->column_count()),
         "Number of column types must match the chunk’s column count.");
  Assert((chunk_encoding_spec.size() == chunk->column_count()),
         "Number of column encoding specs must match the chunk’s column count.");

  std::vector<std::shared_ptr<SegmentStatistics>> column_statistics;
  for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
    const auto spec = chunk_encoding_spec[column_id];

    const auto data_type = column_data_types[column_id];
    const auto base_segment = chunk->get_segment(column_id);
    const auto value_segment = std::dynamic_pointer_cast<const BaseValueSegment>(base_segment);

    Assert(value_segment != nullptr, "All segments of the chunk need to be of type ValueSegment<T>");

    if (spec.encoding_type == EncodingType::Unencoded) {
      // No need to encode, but we still want to have statistics for the now immutable value segment
      column_statistics.push_back(SegmentStatistics::build_statistics(data_type, value_segment));
    } else {
      auto encoded_segment = encode_segment(spec.encoding_type, data_type, value_segment, spec.vector_compression_type);
      chunk->replace_segment(column_id, encoded_segment);
      column_statistics.push_back(SegmentStatistics::build_statistics(data_type, encoded_segment));
    }
  }

  chunk->mark_immutable();
  chunk->set_statistics(std::make_shared<ChunkStatistics>(column_statistics));

  if (chunk->has_mvcc_data()) {
    chunk->get_scoped_mvcc_data_lock()->shrink();
  }
}

void ChunkEncoder::encode_chunk(const std::shared_ptr<Chunk>& chunk, const std::vector<DataType>& column_data_types,
                                const SegmentEncodingSpec& segment_encoding_spec) {
  const auto chunk_encoding_spec = ChunkEncodingSpec{chunk->column_count(), segment_encoding_spec};
  encode_chunk(chunk, column_data_types, chunk_encoding_spec);
}

void ChunkEncoder::encode_chunks(const std::shared_ptr<Table>& table, const std::vector<ChunkID>& chunk_ids,
                                 const std::map<ChunkID, ChunkEncodingSpec>& chunk_encoding_specs) {
  const auto column_data_types = table->column_data_types();

  for (auto chunk_id : chunk_ids) {
    Assert(chunk_id < table->chunk_count(), "Chunk with given ID does not exist.");

    auto chunk = table->get_chunk(chunk_id);
    const auto& chunk_encoding_spec = chunk_encoding_specs.at(chunk_id);

    encode_chunk(chunk, column_data_types, chunk_encoding_spec);
  }
}

void ChunkEncoder::encode_chunks(const std::shared_ptr<Table>& table, const std::vector<ChunkID>& chunk_ids,
                                 const SegmentEncodingSpec& segment_encoding_spec) {
  const auto column_data_types = table->column_data_types();

  for (auto chunk_id : chunk_ids) {
    Assert(chunk_id < table->chunk_count(), "Chunk with given ID does not exist.");
    auto chunk = table->get_chunk(chunk_id);

    encode_chunk(chunk, column_data_types, segment_encoding_spec);
  }
}

void ChunkEncoder::encode_all_chunks(const std::shared_ptr<Table>& table,
                                     const std::vector<ChunkEncodingSpec>& chunk_encoding_specs) {
  const auto column_types = table->column_data_types();
  const auto chunk_count = static_cast<size_t>(table->chunk_count());
  Assert(chunk_encoding_specs.size() == chunk_count, "Number of encoding specs must match table’s chunk count.");

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);
    const auto chunk_encoding_spec = chunk_encoding_specs[chunk_id];

    encode_chunk(chunk, column_types, chunk_encoding_spec);
  }
}

void ChunkEncoder::encode_all_chunks(const std::shared_ptr<Table>& table,
                                     const ChunkEncodingSpec& chunk_encoding_spec) {
  Assert(chunk_encoding_spec.size() == table->column_count(),
         "Number of encoding specs must match table’s column count.");
  const auto column_types = table->column_data_types();

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);
    encode_chunk(chunk, column_types, chunk_encoding_spec);
  }
}

void ChunkEncoder::encode_all_chunks(const std::shared_ptr<Table>& table,
                                     const SegmentEncodingSpec& segment_encoding_spec) {
  const auto column_types = table->column_data_types();

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);

    encode_chunk(chunk, column_types, segment_encoding_spec);
  }
}

}  // namespace opossum
