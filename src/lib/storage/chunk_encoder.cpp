#include "chunk_encoder.hpp"

#include <memory>
#include <thread>
#include <vector>

#include "base_value_segment.hpp"
#include "chunk.hpp"
#include "table.hpp"
#include "types.hpp"

#include "statistics/generate_pruning_statistics.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Function takes an arbitrary segment and (re-)encodes it. This reencoding can both mean that
 * an encoded segment is being recreated as an uncompressed segment (created on the fly via
 * an AnySegmentIterable) or that another encoding is applied (via the segment encoding utils).
 */
std::shared_ptr<BaseSegment> ChunkEncoder::encode_segment(const std::shared_ptr<BaseSegment>& segment,
                                                          const DataType data_type,
                                                          const SegmentEncodingSpec& encoding_spec) {
  std::shared_ptr<BaseSegment> result;
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;

    // TODO(anyone): After #1489, build segment statistics in encode_segment() instead of encode_chunk()
    // and store them within the segment instead of a chunk-owned list of statistics.

    if (const auto reference_segment = std::dynamic_pointer_cast<const ReferenceSegment>(segment)) {
      Fail("Reference segments cannot be encoded.");
    }

    // Check if early exit is possible when passed segment is currently unencoded and the same is requested.
    const auto unencoded_segment = std::dynamic_pointer_cast<const ValueSegment<ColumnDataType>>(segment);
    if (unencoded_segment && encoding_spec.encoding_type == EncodingType::Unencoded) {
      result = segment;
      return;
    }

    // Check for early exit, when requested segment encoding is already being used for the passed segment.
    const auto encoded_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
    if (encoded_segment && encoded_segment->encoding_type() == encoding_spec.encoding_type) {
      // Encoded segments do not need to be reencoded when the requested encoding is already present and either
      // (i) no vector compression is given or (ii) the vector encoding is also already present. Hence, with
      // {EncodingType::Dictionary} being in place, {EncodingType::Dictionary, VectorCompressionType::SimdBp128}
      // does not reencode the segment.
      if (!encoding_spec.vector_compression_type ||
          (encoding_spec.vector_compression_type && encoded_segment->compressed_vector_type() &&
           *encoding_spec.vector_compression_type ==
               parent_vector_compression_type(*encoded_segment->compressed_vector_type()))) {
        result = segment;
        return;
      }
    }

    // In case of unencoded re-encoding, an AnySegmentIterable iterable is used to manually setup
    // the data vectors for a ValueSegment. If another encoding is requested, the segment
    // encoding utitilies are used (which create and call the according encoder).
    if (encoding_spec.encoding_type == EncodingType::Unencoded) {
      pmr_concurrent_vector<ColumnDataType> values;
      pmr_concurrent_vector<bool> null_values;

      auto iterable = create_any_segment_iterable<ColumnDataType>(*segment);
      iterable.with_iterators([&](auto it, auto end) {
        const auto segment_size = std::distance(it, end);
        values.resize(segment_size);
        null_values.resize(segment_size);

        for (auto current_position = size_t{0}; it != end; ++it, ++current_position) {
          const auto segment_item = *it;
          const auto is_null = segment_item.is_null();
          null_values[current_position] = is_null;
          if (!is_null) {
            values[current_position] = segment_item.value();
          } else {
            values[current_position] = {};
          }
        }
      });
      result = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
    } else {
      result = encode_and_compress_segment(segment, data_type, encoding_spec);
    }
  });
  return result;
}

void ChunkEncoder::encode_chunk(const std::shared_ptr<Chunk>& chunk, const std::vector<DataType>& column_data_types,
                                const ChunkEncodingSpec& chunk_encoding_spec) {
  const auto column_count = chunk->column_count();
  Assert(column_data_types.size() == static_cast<size_t>(column_count),
         "Number of column types must match the chunk’s column count.");
  Assert(chunk_encoding_spec.size() == static_cast<size_t>(column_count),
         "Number of column encoding specs must match the chunk’s column count.");
  Assert(!chunk->is_mutable(), "Only immutable chunks can be encoded.");

  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    const auto spec = chunk_encoding_spec[column_id];

    const auto data_type = column_data_types[column_id];
    const auto base_segment = chunk->get_segment(column_id);

    const auto encoded_segment = encode_segment(base_segment, data_type, spec);
    chunk->replace_segment(column_id, encoded_segment);
  }

  if (!chunk->pruning_statistics()) {
    generate_chunk_pruning_statistics(chunk);
  }

  if (chunk->has_mvcc_data()) {
    // MvccData::shrink() will acquire a write lock itself
    // Calling shrink() after the vectors have already been shrinked (e.g., when reencoding),
    // takes less than one millisecond. Thus the check if already shrunk is not necessary.
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
    const auto chunk = table->get_chunk(chunk_id);
    Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

    const auto& chunk_encoding_spec = chunk_encoding_specs.at(chunk_id);
    encode_chunk(chunk, column_data_types, chunk_encoding_spec);
  }
}

void ChunkEncoder::encode_chunks(const std::shared_ptr<Table>& table, const std::vector<ChunkID>& chunk_ids,
                                 const SegmentEncodingSpec& segment_encoding_spec) {
  const auto column_data_types = table->column_data_types();

  for (auto chunk_id : chunk_ids) {
    Assert(chunk_id < table->chunk_count(), "Chunk with given ID does not exist.");
    const auto chunk = table->get_chunk(chunk_id);
    Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

    encode_chunk(chunk, column_data_types, segment_encoding_spec);
  }
}

void ChunkEncoder::encode_all_chunks(const std::shared_ptr<Table>& table,
                                     const std::vector<ChunkEncodingSpec>& chunk_encoding_specs) {
  const auto column_types = table->column_data_types();
  const auto chunk_count = static_cast<size_t>(table->chunk_count());
  Assert(chunk_encoding_specs.size() == chunk_count, "Number of encoding specs must match table’s chunk count.");

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

    const auto chunk_encoding_spec = chunk_encoding_specs[chunk_id];
    encode_chunk(chunk, column_types, chunk_encoding_spec);
  }
}

void ChunkEncoder::encode_all_chunks(const std::shared_ptr<Table>& table,
                                     const ChunkEncodingSpec& chunk_encoding_spec) {
  Assert(chunk_encoding_spec.size() == static_cast<size_t>(table->column_count()),
         "Number of encoding specs must match table’s column count.");
  const auto column_types = table->column_data_types();

  const auto chunk_count = table->chunk_count();
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

    encode_chunk(chunk, column_types, chunk_encoding_spec);
  }
}

void ChunkEncoder::encode_all_chunks(const std::shared_ptr<Table>& table,
                                     const SegmentEncodingSpec& segment_encoding_spec) {
  const auto column_types = table->column_data_types();

  const auto chunk_count = table->chunk_count();
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

    encode_chunk(chunk, column_types, segment_encoding_spec);
  }
}

}  // namespace opossum
