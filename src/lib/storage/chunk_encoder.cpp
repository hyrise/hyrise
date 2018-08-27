#include "chunk_encoder.hpp"

#include <memory>
#include <vector>

#include "base_value_column.hpp"
#include "chunk.hpp"
#include "table.hpp"
#include "types.hpp"

#include "statistics/chunk_statistics/segment_statistics.hpp"
#include "statistics/chunk_statistics/chunk_statistics.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/column_encoding_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

void ChunkEncoder::encode_chunk(const std::shared_ptr<Chunk>& chunk, const std::vector<DataType>& data_types,
                                const ChunkEncodingSpec& chunk_encoding_spec) {
  Assert((data_types.size() == chunk->cxlumn_count()), "Number of column types must match the chunk’s column count.");
  Assert((chunk_encoding_spec.size() == chunk->cxlumn_count()),
         "Number of column encoding specs must match the chunk’s column count.");

  std::vector<std::shared_ptr<SegmentStatistics>> cxlumn_statistics;
  for (CxlumnID cxlumn_id{0}; cxlumn_id < chunk->cxlumn_count(); ++cxlumn_id) {
    const auto spec = chunk_encoding_spec[cxlumn_id];

    const auto data_type = data_types[cxlumn_id];
    const auto base_column = chunk->get_column(cxlumn_id);
    const auto value_column = std::dynamic_pointer_cast<const BaseValueSegment>(base_column);

    Assert(value_column != nullptr, "All columns of the chunk need to be of type ValueSegment<T>");

    if (spec.encoding_type == EncodingType::Unencoded) {
      // No need to encode, but we still want to have statistics for the now immutable value column
      cxlumn_statistics.push_back(SegmentStatistics::build_statistics(data_type, value_column));
    } else {
      auto encoded_column = encode_column(spec.encoding_type, data_type, value_column, spec.vector_compression_type);
      chunk->replace_column(cxlumn_id, encoded_column);
      cxlumn_statistics.push_back(SegmentStatistics::build_statistics(data_type, encoded_column));
    }
  }

  chunk->mark_immutable();
  chunk->set_statistics(std::make_shared<ChunkStatistics>(cxlumn_statistics));

  if (chunk->has_mvcc_columns()) {
    chunk->get_scoped_mvcc_columns_lock()->shrink();
  }
}

void ChunkEncoder::encode_chunk(const std::shared_ptr<Chunk>& chunk, const std::vector<DataType>& data_types,
                                const ColumnEncodingSpec& column_encoding_spec) {
  const auto chunk_encoding_spec = ChunkEncodingSpec{chunk->cxlumn_count(), column_encoding_spec};
  encode_chunk(chunk, data_types, chunk_encoding_spec);
}

void ChunkEncoder::encode_chunks(const std::shared_ptr<Table>& table, const std::vector<ChunkID>& chunk_ids,
                                 const std::map<ChunkID, ChunkEncodingSpec>& chunk_encoding_specs) {
  const auto data_types = table->column_data_types();

  for (auto chunk_id : chunk_ids) {
    Assert(chunk_id < table->chunk_count(), "Chunk with given ID does not exist.");

    auto chunk = table->get_chunk(chunk_id);
    const auto& chunk_encoding_spec = chunk_encoding_specs.at(chunk_id);

    encode_chunk(chunk, data_types, chunk_encoding_spec);
  }
}

void ChunkEncoder::encode_chunks(const std::shared_ptr<Table>& table, const std::vector<ChunkID>& chunk_ids,
                                 const ColumnEncodingSpec& column_encoding_spec) {
  const auto data_types = table->column_data_types();

  for (auto chunk_id : chunk_ids) {
    Assert(chunk_id < table->chunk_count(), "Chunk with given ID does not exist.");
    auto chunk = table->get_chunk(chunk_id);

    encode_chunk(chunk, data_types, column_encoding_spec);
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
  Assert(chunk_encoding_spec.size() == table->cxlumn_count(),
         "Number of encoding specs must match table’s column count.");
  const auto column_types = table->column_data_types();

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);
    encode_chunk(chunk, column_types, chunk_encoding_spec);
  }
}

void ChunkEncoder::encode_all_chunks(const std::shared_ptr<Table>& table,
                                     const ColumnEncodingSpec& column_encoding_spec) {
  const auto column_types = table->column_data_types();

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);

    encode_chunk(chunk, column_types, column_encoding_spec);
  }
}

}  // namespace opossum
