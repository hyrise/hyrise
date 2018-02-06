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

struct ColumnEncodingSpec {
  EncodingType encoding_type;
  std::optional<VectorCompressionType> vector_compression_type = {};
};

using ChunkEncodingSpec = std::vector<ColumnEncodingSpec>;

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
   * Reduces also the fragmentation of the chunk’s MVCC columns.
   * All columns of the chunk need to be of type ValueColumn<T>,
   * i.e., recompression is not yet supported.
   *
   * Note: In some cases, it might be benificial to
   *       leave certain columns of a chunk unencoded.
   *       Use EncodingType::Unencoded in this case.
   */
  static void encode_chunk(const std::shared_ptr<Chunk>& chunk, const std::vector<DataType>& data_types,
                           const ChunkEncodingSpec& encoding_spec);

  /**
   * @brief Encodes the specified chunks of the passed table
   */
  static void encode_chunks(const std::shared_ptr<Table>& table, const std::vector<ChunkID>& chunk_ids,
                            const std::map<ChunkID, ChunkEncodingSpec>& encoding_specs);

  /**
   * @brief Encodes a complete table
   */
  static void encode_all_chunks(const std::shared_ptr<Table>& table,
                                const std::vector<ChunkEncodingSpec>& encoding_specs);
};

}  // namespace opossum
