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

  explicit LZ4Segment(const std::shared_ptr<const pmr_vector<char>>& compressed_data,
                      const std::shared_ptr<const pmr_vector<bool>>& null_values,
                      const std::shared_ptr<const pmr_vector<size_t>>& offsets,
                      const int compressed_size,
                      const int decompressed_size,
                      const size_t num_elements);

  std::shared_ptr<const pmr_vector<char>> compressed_data() const;
  std::shared_ptr<const pmr_vector<bool>> null_values() const;
  std::shared_ptr<const pmr_vector<size_t>> offsets() const;
  int compressed_size() const;
  int decompressed_size() const;

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
  std::optional<CompressedVectorType> compressed_vector_type() const final;

  /**@}*/

 private:
  const std::shared_ptr<const pmr_vector<char>> _compressed_data;
  const std::shared_ptr<const pmr_vector<bool>> _null_values;
  const std::shared_ptr<const pmr_vector<size_t>> _offsets;
  const int _compressed_size;
  const int _decompressed_size;
  size_t _num_elements;
};

}  // namespace opossum
