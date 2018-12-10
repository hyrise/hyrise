#pragma once

#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

#include "storage/encoding_type.hpp"
#include "storage/vector_compression/vector_compression.hpp"

namespace opossum {

class Chunk;
class Table;

struct SegmentEncodingSpec {
  constexpr SegmentEncodingSpec() : encoding_type{EncodingType::Dictionary} {}
  constexpr SegmentEncodingSpec(EncodingType encoding_type_) : encoding_type{encoding_type_} {}
  constexpr SegmentEncodingSpec(EncodingType encoding_type_, VectorCompressionType vector_compression_type_)
      : encoding_type{encoding_type_}, vector_compression_type{vector_compression_type_} {}

  EncodingType encoding_type;
  std::optional<VectorCompressionType> vector_compression_type;
};

using ChunkEncodingSpec = std::vector<SegmentEncodingSpec>;

/**
 * @brief Interface for encoding chunks
 *
 * The methods provided are not thread-safe and might lead to race conditions
 * if there are other operations manipulating the chunks at the same time.
 */
class ChunkEncoder {
 public:
  /**
   * @brief Encodes a chunk
   *
   * Encodes a chunk using the passed encoding specifications.
   * Reduces also the fragmentation of the chunk’s MVCC data.
   * All segments of the chunk need to be of type ValueSegment<T>,
   * i.e., recompression is not yet supported.
   *
   * Note: In some cases, it might be beneficial to
   *       leave certain segments of a chunk unencoded.
   *       Use EncodingType::Unencoded in this case.
   */
  static void encode_chunk(const std::shared_ptr<Chunk>& chunk, const std::vector<DataType>& column_data_types,
                           const ChunkEncodingSpec& chunk_encoding_spec);

  /**
   * @brief Encodes a chunk using the same segment-encoding spec
   */
  static void encode_chunk(const std::shared_ptr<Chunk>& chunk, const std::vector<DataType>& column_data_types,
                           const SegmentEncodingSpec& segment_encoding_spec = {});

  /**
   * @brief Encodes the specified chunks of the passed table
   *
   * The encoding is specified per segment for each chunk.
   */
  static void encode_chunks(const std::shared_ptr<Table>& table, const std::vector<ChunkID>& chunk_ids,
                            const std::map<ChunkID, ChunkEncodingSpec>& chunk_encoding_specs);

  /**
   * @brief Encodes the specified chunks of the passed table using a single segment-encoding spec
   */
  static void encode_chunks(const std::shared_ptr<Table>& table, const std::vector<ChunkID>& chunk_ids,
                            const SegmentEncodingSpec& segment_encoding_spec = {});

  /**
   * @brief Encodes an entire table
   *
   * The encoding is specified per segment for each chunk.
   */
  static void encode_all_chunks(const std::shared_ptr<Table>& table,
                                const std::vector<ChunkEncodingSpec>& chunk_encoding_specs);

  /**
   * @brief Encodes an entire table
   *
   * The encoding is specified per segment and is the same for each chunk.
   */
  static void encode_all_chunks(const std::shared_ptr<Table>& table, const ChunkEncodingSpec& chunk_encoding_spec);

  /**
   * @brief Encodes an entire table using a single segment-encoding spec
   */
  static void encode_all_chunks(const std::shared_ptr<Table>& table,
                                const SegmentEncodingSpec& segment_encoding_spec = {});
};

}  // namespace opossum
