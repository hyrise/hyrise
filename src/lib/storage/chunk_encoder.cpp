#include "chunk_encoder.hpp"

#include <memory>
#include <thread>
#include <vector>

#include "base_value_segment.hpp"
#include "chunk.hpp"
#include "resolve_type.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/base_segment_encoder.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment.hpp"
#include "table.hpp"
#include "types.hpp"
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
  Assert(!std::dynamic_pointer_cast<const ReferenceSegment>(segment), "Reference segments cannot be encoded.");

  std::shared_ptr<BaseSegment> result;
  resolve_data_type(data_type, [&](const auto type) {
    using ColumnDataType = typename decltype(type)::type;

    // TODO(anyone): After #1489, build segment statistics in encode_segment() instead of encode_chunk()
    // and store them within the segment instead of a chunk-owned list of statistics.

    // Check if early exit is possible when passed segment is already encoded with requested spec.
    // In case no vector compression is specified, only the correct encoding type is checked and the current vector
    // compression type is ignored.
    const auto current_segment_encoding_spec = get_segment_encoding_spec(segment);
    if (current_segment_encoding_spec == encoding_spec ||
        (!encoding_spec.vector_compression_type &&
         current_segment_encoding_spec.encoding_type == encoding_spec.encoding_type)) {
      result = segment;
      return;
    }

    // In case of unencoded re-encoding, an AnySegmentIterable iterable is used to manually setup
    // the data vectors for a ValueSegment. If another encoding is requested, the segment
    // encoding utitilies are used (which create and call the according encoder).
    if (encoding_spec.encoding_type == EncodingType::Unencoded) {
      pmr_vector<ColumnDataType> values;
      pmr_vector<bool> null_values;

      auto iterable = create_any_segment_iterable<ColumnDataType>(*segment);
      iterable.with_iterators([&](auto it, const auto end) {
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
      auto encoder = create_encoder(encoding_spec.encoding_type);
      if (encoding_spec.vector_compression_type) {
        encoder->set_vector_compression(*encoding_spec.vector_compression_type);
      }

      result = encoder->encode(segment, data_type);
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

  generate_chunk_pruning_statistics(chunk);
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
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

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
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

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
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

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
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    encode_chunk(chunk, column_types, chunk_encoding_spec);
  }
}

void ChunkEncoder::encode_all_chunks(const std::shared_ptr<Table>& table,
                                     const SegmentEncodingSpec& segment_encoding_spec) {
  const auto column_types = table->column_data_types();

  const auto chunk_count = table->chunk_count();
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    encode_chunk(chunk, column_types, segment_encoding_spec);
  }
}

}  // namespace opossum
