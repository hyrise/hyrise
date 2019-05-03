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
class BaseSegment;

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
 * NOT thread-safe. In a multi-threaded context, the ChunkCompressionTask should invoke the ChunkEncoder.
 *
 * The methods provided are not thread-safe and might lead to race conditions
 * if there are other operations manipulating the chunks at the same time.
 */
class ChunkEncoder {
 public:
  static std::shared_ptr<BaseSegment> encode_segment(const std::shared_ptr<BaseSegment>& segment, const DataType data_type,
                                                     const SegmentEncodingSpec& encoding_spec);

  /**
   * @brief Encodes a chunk
   *
   * Encodes a chunk using the passed encoding specifications.
   * Reduces also the fragmentation of the chunk’s MVCC data.
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
