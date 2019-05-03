#include "chunk_encoder.hpp"

#include <memory>
#include <vector>

#include "base_value_segment.hpp"
#include "chunk.hpp"
#include "table.hpp"
#include "types.hpp"

#include "statistics/chunk_statistics/chunk_statistics.hpp"
#include "statistics/chunk_statistics/segment_statistics.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<BaseSegment> ChunkEncoder::encode_segment(const std::shared_ptr<BaseSegment>& segment,
                                                          const DataType data_type,
                                                          const SegmentEncodingSpec& encoding_spec) {
  std::shared_ptr<BaseSegment> return_segment;
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;

    // TODO(anyone): After #1489, build segment statistics in encode_segment() instead of encode_chunk() and stored them
    // within the segment instead of a chunk-owned list of statistics.

    // Early exits when desired encoding is already in place.
    const auto encoded_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
    if (encoded_segment && encoded_segment->encoding_type() == encoding_spec.encoding_type) {
      // Encoded segments to not need to be reencoded when the requested encoding is already present and either
      // (i) no vector compression is given or (ii) the vector encoding is also already present. Hence, with
      // {EncodingType::Dictionary} being in place, {EncodingType::Dictionary, VectorCompressionType::SimdBp128}
      // does not reencode the segment.
      if (!encoding_spec.vector_compression_type ||
          (encoding_spec.vector_compression_type && encoded_segment->compressed_vector_type() &&
           encoding_spec.vector_compression_type ==
               parent_vector_compression_type(*encoded_segment->compressed_vector_type()))) {
        return_segment = segment;
        return;
      }
    }

    const auto unencoded_segment = std::dynamic_pointer_cast<const ValueSegment<ColumnDataType>>(segment);
    if (unencoded_segment && encoding_spec.encoding_type == EncodingType::Unencoded) {
      return_segment = segment;
      return;
    }

    if (encoding_spec.encoding_type == EncodingType::Unencoded) {
      pmr_concurrent_vector<ColumnDataType> values;
      pmr_concurrent_vector<bool> null_values;

      auto iterable = create_any_segment_iterable<ColumnDataType>(*segment);
      iterable.with_iterators([&](auto it, auto end) {
        const auto segment_size = std::distance(it, end);
        values.reserve(segment_size);
        null_values.resize(segment_size);

        for (; it != end; ++it) {
          const auto segment_item = *it;
          const auto is_null = segment_item.is_null();
          null_values.push_back(is_null);
          if (!is_null) {
            values.push_back(segment_item.value());
          } else {
            values.push_back({});
          }
        }
      });
      return_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
    } else {
      // TODO(anyone): unsure why ::opossum:: is needed here
      return_segment = ::opossum::encode_segment(encoding_spec.encoding_type, data_type, segment,
                                                 encoding_spec.vector_compression_type);
    }
  });
  return return_segment;
}

void ChunkEncoder::encode_chunk(const std::shared_ptr<Chunk>& chunk, const std::vector<DataType>& column_data_types,
                                const ChunkEncodingSpec& chunk_encoding_spec) {
  Assert((column_data_types.size() == chunk->column_count()),
         "Number of column types must match the chunk’s column count.");
  Assert((chunk_encoding_spec.size() == chunk->column_count()),
         "Number of column encoding specs must match the chunk’s column count.");

  const auto chunk_statistics = chunk->statistics();
  std::vector<std::shared_ptr<SegmentStatistics>> column_statistics;
  for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
    const auto spec = chunk_encoding_spec[column_id];

    const auto data_type = column_data_types[column_id];
    const auto base_segment = chunk->get_segment(column_id);

    const auto encoded_segment = encode_segment(base_segment, data_type, spec);
    chunk->replace_segment(column_id, encoded_segment);

    if (!chunk_statistics) {
      column_statistics.push_back(SegmentStatistics::build_statistics(data_type, encoded_segment));
    }
  }

  chunk->mark_immutable();
  if (!chunk_statistics) {
    chunk->set_statistics(std::make_shared<ChunkStatistics>(column_statistics));
  }

  if (chunk->has_mvcc_data() && !chunk->mvcc_data()->is_shrunk()) {
    // MvccData::shrink() will acquire a write lock itself
    chunk->mvcc_data()->shrink();
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
