#pragma once

#include <array>
#include <memory>
#include <type_traits>

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include "base_encoded_segment.hpp"
#include "storage/pos_list.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"
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
   * power of two. That means that the row values perfectly fit into a block (whose size is also a power-of-two) and no
   * value is split across two blocks. This makes decompression very convenient.
   *
   * @param lz4_blocks A vector that contains every LZ4 block separately (i.e., this is a vector of vectors). The blocks
   *                   are stored in this data format since they are created independently and are also accessed
   *                   independently. The decompressed size of the first n - 1 blocks is "block_size" and the
   *                   decompressed size of the last vector is equal to "last_block_size".
   * @param null_values Boolean vector that contains the information which row is null and which is not null. If no
   *                    value in the segment is null, std::nullopt is passed instead to reduce the memory footprint of
   *                    the vector.
   * @param dictionary This dictionary should be generated via the zstd library. It is used to initialize the LZ4
   *                   stream compression algorithm. Doing that makes the compression of separate blocks independent of
   *                   each other (by default the blocks would depend on the previous blocks). If the segment only has
   *                   a single block, the passed dictionary will be empty since it is not needed for independent
   *                   decompression.
   * @param block_size The decompressed size of each full block in bytes. This can be numeric_limits<int>::max() at max.
   * @param last_block_size The size of the last block in bytes. It is a separate value since the last block is not
   *                        necessarily full.
   * @param compressed_size The sum of the compressed size of all blocks. This is a separate argument, so that
   *                        there is no need to iterate over all blocks when estimating the memory usage.
   * @param num_elements The number of elements in this segment. This needs to be stored in its own variable, since
   *                     the other variables might not be set or stored to reduce the memory footprint. E.g., a string
   *                     segment with only empty strings as elements would have no other way to know how many rows there
   *                     are.
   */
  explicit LZ4Segment(pmr_vector<pmr_vector<char>>&& lz4_blocks, std::optional<pmr_vector<bool>>&& null_values,
                      pmr_vector<char>&& dictionary, const size_t block_size, const size_t last_block_size,
                      const size_t compressed_size, const size_t num_elements);

  /**
   * This constructor is used only for pmr_string segments. In those, the size of each row value varies. This means that
   * a row value can be split into multiple blocks (even more than two if the value is larger than the block size). That
   * makes the decompression slightly more complex.
   *
   * @param lz4_blocks A vector that contains every LZ4 block separately (i.e., this is a vector of vectors). The blocks
   *                   are stored in this data format since they are created independently and are also accessed
   *                   independently. The decompressed size of the first n - 1 blocks is "block_size" and the
   *                   decompressed size of the last vector is equal to "last_block_size".
   * @param null_values Boolean vector that contains the information which row is null and which is not null. If no
   *                    value in the segment is null, std::nullopt is passed instead to reduce the memory footprint of
   *                    the vector.
   * @param dictionary This dictionary should be generated via the zstd library. It is used to initialize the LZ4
   *                   stream compression algorithm. Doing that makes the compression of separate blocks independent of
   *                   each other (by default, the blocks would depend on the previous blocks). If the segment only has
   *                   a single block, the passed dictionary will be empty since it is not needed for independent
   *                   decompression.
   * @param string_offsets These offsets are only needed if this segment is not a pmr_string segment.
   *                       Otherwise, this is set to a std::nullopt (see the other constructor).
   *                       It contains the offsets for the compressed strings. The offset at position 0 is the
   *                       character index of the string at index 0. Its (exclusive) end is at the offset at position 1.
   *                       The last string ends at the end of the compressed data (since there is an offset after it
   *                       that specifies the end offset). Since these offsets are used, the stored strings are not
   *                       null-terminated (and may contain null bytes).
   *                       The offsets are compressed using a vector compression method to reduce their memory footprint.
   * @param block_size The decompressed size of each full block in bytes. This can be numeric_limits<int>::max() at max.
   * @param last_block_size The size of the last block in bytes. It is a separate value since the last block is not
   *                        necessarily full.
   * @param compressed_size The sum of the compressed size of all blocks. This is a separate argument so that
   *                        there is no need to iterate over all blocks when estimating the memory usage.
   * @param num_elements The number of elements in this segment. This needs to be stored in its own variable, since
   *                     the other variables might not be set or stored to reduce the memory footprint. E.g., a string
   *                     segment with only empty strings as elements would have no other way to know how many rows there
   *                     are.
   */
  explicit LZ4Segment(pmr_vector<pmr_vector<char>>&& lz4_blocks, std::optional<pmr_vector<bool>>&& null_values,
                      pmr_vector<char>&& dictionary, std::unique_ptr<const BaseCompressedVector>&& string_offsets,
                      const size_t block_size, const size_t last_block_size, const size_t compressed_size,
                      const size_t num_elements);

  const std::optional<pmr_vector<bool>>& null_values() const;
  std::optional<std::unique_ptr<BaseVectorDecompressor>> string_offset_decompressor() const;
  const pmr_vector<char>& dictionary() const;
  const pmr_vector<pmr_vector<char>>& lz4_blocks() const;
  size_t block_size() const;
  size_t last_block_size() const;
  const std::optional<std::unique_ptr<const BaseCompressedVector>>& string_offsets() const;

  /**
   * @defgroup BaseSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;

  ChunkOffset size() const final;

  /**
   * Decompresses the whole segment at once into a single vector.
   *
   * @return A vector containing all the decompressed values in order.
   */
  std::vector<T> decompress() const;

  /**
   * Retrieves a single value by only decompressing the block in resides in. Each call of this method causes the
   * decompression of a block.
   *
   * @param chunk_offset The chunk offset identifies a single value in the segment.
   * @return The decompressed value.
   */
  T decompress(const ChunkOffset& chunk_offset) const;

  /**
   * Retrieves a single value by only decompressing the block in resides in. This method also accepts a previously
   * decompressed block (and its block index) to check if the queried value also resides in that block. If that is the
   * case, the value is retrieved directly instead of decompressing the block again.
   * If the passed block is a different block, it is overwritten with the newly decompressed block.
   * This block is stored (and passed) as char-vector instead of type T to maintain compatibility with string-segments,
   * since those don't compress a string-vector but a char-vector. In the case of non-string-segments, the data will be
   * cast to type T. In the case of string-segments, the char-vector can be used directly.
   *
   * @param chunk_offset The chunk offset identifies a single value in the segment.
   * @param cached_block_index The index of the passed decompressed block. Passing a nullopt indicates that there is
   *                             no previous block that was decompressed. In that case the newly decompressed block is
   *                             written to the passed vector. This is only the case for the first decompression, when
   *                             resolving a position list in the point access iterator.
   * @param cached_block Vector that contains a previously decompressed block. If this method needs to access a
   *                       different block, the data is overwritten.
   * @return A pair of the decompressed value and the index of the block it resides in. This index is the same as the
   *         input index if no new block had to be decompressed. Otherwise it is the index of the block that was written
   *         to the passed vector.
   */
  std::pair<T, size_t> decompress(const ChunkOffset& chunk_offset, const std::optional<size_t> cached_block_index,
                                  std::vector<char>& cached_block) const;

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
  const std::optional<pmr_vector<bool>> _null_values;
  const pmr_vector<char> _dictionary;
  const std::optional<std::unique_ptr<const BaseCompressedVector>> _string_offsets;
  const size_t _block_size;
  const size_t _last_block_size;
  const size_t _compressed_size;
  const size_t _num_elements;

  /**
   * Decompress a single block into the provided buffer (the vector). This method writes to the buffer with the given
   * offset, i.e., the buffer can be larger than a single block.
   *
   * @param block_index Index of the block in _lz4_blocks that is decompressed.
   * @param decompressed_data The buffer to which the decompressed data is written.
   * @param write_offset Byte offset from the beginning of the decompressed_data vector. This is useful when
   *                     decompressing multiple blocks into the same buffer.
   */
  void _decompress_block(const size_t block_index, std::vector<T>& decompressed_data, const size_t write_offset) const;

  /**
   * Decompresses a single block into a char vector. This method resizes the input vector if the decompressed data
   * would not fit into it. It is used for string-segments as well as non-string-segments.
   * This allows a uniform interface in the decompress method for caching. For non-string-segments the decompressed
   * values have to be further cast to type T, while string-segments can use the char-vector directly.
   *
   * @param block_index Index of the block that is decompressed.
   * @param decompressed_data Vector to which the decompressed data is written. This data is written in bytes (i.e.
   *                          char) and needs to be cast to type T to get the proper values.
   */
  void _decompress_block_to_bytes(const size_t block_index, std::vector<char>& decompressed_data) const;

  /**
   * Decompress a single block into bytes. For strings the bytes equal the chars of the strings. This method uses the
   * passed offset to write into the passed char-vector with that offset. I.e., it allows to decompress multiple blocks
   * into the same vector.
   *
   * @param block_index Index of the block that is decompressed.
   * @param decompressed_data Vector to which the decompressed data is written. Its size needs to be at least equal to
   *                          the write offset + the decompressed size of the block.
   * @param write_offset The byte offset to which is written in the passed vector.
   */
  void _decompress_block_to_bytes(const size_t block_index, std::vector<char>& decompressed_data,
                                  const size_t write_offset) const;
};

}  // namespace opossum
