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

  explicit LZ4Segment(const int decompressed_size, const int max_compressed_size,
                      std::shared_ptr<std::vector<char>> compressed_data);

  int decompressed_size() const;
  int max_compressed_size() const;
  const std::vector<char>& compressed_data() const;

  /**
   * @defgroup BaseSegment interface
   * @{
   */

  const AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  const std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;

  size_t size() const final;

  std::shared_ptr<std::vector<T>> decompress() const;

  std::shared_ptr<BaseSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  size_t estimate_memory_usage() const final;

  /**@}*/

  /**
   * @defgroup BaseEncodedSegment interface
   * @{
   */

  EncodingType encoding_type() const final;

  /**@}*/

 private:
  const int _decompressed_size;
  const int _max_compressed_size;
  const std::shared_ptr<std::vector<char>> _compressed_data;
};

}  // namespace opossum
