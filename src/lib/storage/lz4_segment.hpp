#pragma once

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include <type_traits>

#include <array>
#include <memory>

#include "base_encoded_segment.hpp"
#include "storage/pos_list.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "types.hpp"

namespace opossum {

class BaseCompressedVector;

template <typename T>
class LZ4Segment : public BaseEncodedSegment {
 public:
  /**
   * This is a container for an LZ4 compressed segment. It contains the compressed data in blocks, the necessary
   * metadata and the ability to decompress the data again.
   *
   * This constructor is used for non pmr_string segments. In those, the size of the data type in bytes is a
   * power-of-two. That means that the row values perfectly fit into a block (whose size is also a power-of-two) and no
   * value is split across two blocks. This makes decompression very convenient.
   *
   * @param lz4_blocks A vector that contains every LZ4 block separately (i.e., this is a vector of vectors). The blocks
   *                   are stored in this data format since they are created independently and are also accessed
   *                   independently. The decompressed size of the first n - 1 blocks is "block_size" and the
   *                   decompressed size of the last vector is equal to "last_block_size".
   * @param null_values Boolean vector that contains the information which row is null and which is not null.
   * @param dictionary This dictionary should be generated via the zstd library. It is used to initialize the LZ4
   *                   stream compression algorithm. Doing that makes the compression of separate blocks indepedent of
   *                   each other (by default the blocks would depend on the previous blocks). If the segment only has
   *                   a single block the passed dictionary will be emtpy since it does not needed for independent
   *                   decompression.
   * @param block_size The decompressed size of each full block in bytes. This can be numeric_limits<int>::max() at max.
   * @param last_block_size The size of the last block in bytes. It is a separate value since the last block is not
   *                        necessarily full.
   * @param compressed_size The sum of the compressed size of all blocks. This is a separate argument so that
   *                        there is no need to iterate over all blocks when estimating the memory usage.
   */
  explicit LZ4Segment(pmr_vector<pmr_vector<char>>&& lz4_blocks, pmr_vector<bool>&& null_values,
                      pmr_vector<char>&& dictionary, const size_t block_size, const size_t last_block_size,
                      const size_t compressed_size);

  /**
   * This is a container for an LZ4 compressed segment. It contains the compressed data in blocks, the necessary
   * metadata and the ability to decompress the data again.
   *
   * This constructor is used only for pmr_string segments. In those, the size of each row value varies. This means that
   * a row value can be split into multiple blocks (even more than two if the value is larger than the block size). That
   * makes the decompression slightly more complex.
   *
   * @param lz4_blocks A vector that contains every LZ4 block separately (i.e., this is a vector of vectors). The blocks
   *                   are stored in this data format since they are created independently and are also accessed
   *                   independently. The decompressed size of the first n - 1 blocks is "block_size" and the
   *                   decompressed size of the last vector is equal to "last_block_size".
   * @param null_values Boolean vector that contains the information which row is null and which is not null.
   * @param dictionary This dictionary should be generated via the zstd library. It is used to initialize the LZ4
   *                   stream compression algorithm. Doing that makes the compression of separate blocks indepedent of
   *                   each other (by default the blocks would depend on the previous blocks). If the segment only has
   *                   a single block the passed dictionary will be emtpy since it does not needed for independent
   *                   decompression.
   * @param string_offsets These offsets are only needed if this segment is not a pmr_string segment.
   *                       Otherwise this is set to a std::nullopt (see the other constructor).
   *                       It contains the offsets for the compressed strings. The offset at position 0 is the
   *                       character index of the string at index 0. Its (exclusive) end is at the offset at position 1.
   *                       The last string ends at the end of the compressed data (since there is offset after it that
   *                       specifies the end offset). Since these offsets are used, the stored strings are not
   *                       null-terminated (and may contain null bytes).
   * @param block_size The decompressed size of each full block in bytes. This can be numeric_limits<int>::max() at max.
   * @param last_block_size The size of the last block in bytes. It is a separate value since the last block is not
   *                        necessarily full.
   * @param compressed_size The sum of the compressed size of all blocks. This is a separate argument so that
   *                        there is no need to iterate over all blocks when estimating the memory usage.
   */
  explicit LZ4Segment(pmr_vector<pmr_vector<char>>&& lz4_blocks, pmr_vector<bool>&& null_values,
                      pmr_vector<char>&& dictionary, pmr_vector<size_t>&& string_offsets, const size_t block_size,
                      const size_t last_block_size, const size_t compressed_size);

  const pmr_vector<bool>& null_values() const;
  const std::optional<const pmr_vector<size_t>> string_offsets() const;
  const pmr_vector<char>& dictionary() const;

  /**
   * @defgroup BaseSegment interface
   * @{
   */

  const AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  const std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;

  size_t size() const final;

  std::vector<T> decompress() const;
  T decompress(const ChunkOffset& chunk_offset) const;

  std::shared_ptr<BaseSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  size_t estimate_memory_usage() const final;

  /**@}*/

  /**
   * @defgroup BaseEncodedSegment interface
   * @{
   */

  EncodingType encoding_type() const final;
  std::optional<CompressedVectorType> compressed_vector_type() const final;

  /**@}*/

 private:
  const pmr_vector<pmr_vector<char>> _lz4_blocks;
  const pmr_vector<bool> _null_values;
  const pmr_vector<char> _dictionary;
  const std::optional<const pmr_vector<size_t>> _string_offsets;
  const size_t _block_size;
  const size_t _last_block_size;
  const size_t _compressed_size;

  /**
   * Decompress a single block into the provided buffer (the vector). This method writes to the buffer with the given
   * offset, i.e. the buffer can be larger than a single block.
   *
   * @param block_index Index of the block in _lz4_blocks that is decompressed.
   * @param decompressed_data The buffer to which the decompressed data is written.
   * @param write_offset Byte offset from the beginning of the decompressed_data vector. This is useful when
   *                     decompressing multiple blocks into the same buffer.
   */
  void _decompress_block(const size_t block_index, std::vector<T>& decompressed_data, const size_t write_offset) const;

  void _decompress_string_block(const size_t block_index, std::vector<char>& decompressed_data) const;
  void _decompress_string_block(const size_t block_index, std::vector<char>& decompressed_data,
                                const size_t write_offset) const;
};

}  // namespace opossum
