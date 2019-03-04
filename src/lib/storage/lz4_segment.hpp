#pragma once

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include <type_traits>

#include <array>
#include <memory>

#include "base_encoded_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "types.hpp"

namespace opossum {

class BaseCompressedVector;

template <typename T>
class LZ4Segment : public BaseEncodedSegment {
 public:
  /*
   * This is a container for an LZ4 compressed segment. It contains the compressed data, the necessary
   * metadata and the ability to decompress the data again.
   *
   * @param compressed_data The char vector that contains the LZ4 compressed segment data as binary blob.
   * @param null_values Boolean vector that contains the information which row is null and which is not null.
   * @param offsets If this segment is not a pmr_string segment this will be a std::nullopt (see the other constructor).
   *                Otherwise it contains the offsets for the compressed strings. The offset at position 0 is the
   *                character index of the string at index 0. Its (exclusive) end is at the offset at position 1. The
   *                last string ends at the end of the compressed data (since there is offset after it that specifies
   *                the end offset). Since these offsets are used the stored strings are not null-terminated
   *                (and may contain null bytes).
   * @param compressed_size The size of the compressed data vector (the return value of LZ4)
   * @param decompressed_size The size in bytes of the decompressed data vector.
   */
  explicit LZ4Segment(pmr_vector<char>&& compressed_data, pmr_vector<bool>&& null_values, pmr_vector<size_t>&& offsets,
                      const size_t decompressed_size);

  explicit LZ4Segment(pmr_vector<char>&& compressed_data, pmr_vector<bool>&& null_values,
                      const size_t decompressed_size);

  const pmr_vector<bool>& null_values() const;
  const std::optional<const pmr_vector<size_t>> offsets() const;

  /**
   * @defgroup BaseSegment interface
   * @{
   */

  const AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  const std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;

  size_t size() const final;

  std::vector<T> decompress() const;

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
  const pmr_vector<char> _compressed_data;
  const pmr_vector<bool> _null_values;
  const std::optional<const pmr_vector<size_t>> _offsets;
  const size_t _decompressed_size;
};

}  // namespace opossum
