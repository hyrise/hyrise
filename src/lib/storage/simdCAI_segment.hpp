#pragma once

#include <memory>
#include <type_traits>

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include "include/codecfactory.h"
#include "include/intersection.h"
#include "include/frameofreference.h"

#include "abstract_encoded_segment.hpp"
#include "types.hpp"

namespace opossum {

class BaseCompressedVector;

/**
 * @brief Segment for encodings from the SIMDCompressionAndIntersection lib.
 *
 * todo: docs
 *
 * std::enable_if_t must be used here and cannot be replaced by a
 * static_assert in order to prevent instantiation of
 * FrameOfReferenceSegment<T> with T other than int32_t. Otherwise,
 * the compiler might instantiate FrameOfReferenceSegment with other
 * types even if they are never actually needed.
 * "If the function selected by overload resolution can be determined
 * without instantiating a class template definition, it is unspecified
 * whether that instantiation actually takes place." Draft Std. N4800 12.8.1.8
 */
template <typename T, typename = std::enable_if_t<encoding_supports_data_type(
    enum_c<EncodingType, EncodingType::SIMDCAI>, hana::type_c<T>)>>
class SIMDCAISegment : public AbstractEncodedSegment {
 public:
  explicit SIMDCAISegment(const std::shared_ptr<const pmr_vector<uint32_t>>& encoded_values,
                           std::optional<pmr_vector<bool>> null_values,
                           uint8_t codec_id,
                           ChunkOffset size);

  const std::shared_ptr<const pmr_vector<uint32_t>> encoded_values() const;
  const std::optional<pmr_vector<bool>>& null_values() const;
  uint8_t codec_id() const;
  ChunkOffset size() const final;

  /**
   * @defgroup AbstractSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const {
    // performance critical - not in cpp to help with inlining

    if (_null_values && (*_null_values)[chunk_offset]) {
      return std::nullopt;
    }

    auto decoded_values = std::vector<uint32_t>(_size);
    size_t recovered_size = decoded_values.size();

    // SIMDCompressionLib::FrameOfReference codec;
    SIMDCompressionLib::IntegerCODEC &codec = *SIMDCompressionLib::CODECFactory::getFromName("simdframeofreference");

    codec.decodeArray(_encoded_values->data(), _encoded_values->size(), decoded_values.data(), recovered_size);

    return static_cast<T>(decoded_values[chunk_offset]);
    //return static_cast<T>(codec.select(_encoded_values->data(), chunk_offset));
  }

  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  size_t memory_usage(const MemoryUsageCalculationMode mode) const final;

  /**@}*/

  /**
   * @defgroup AbstractEncodedSegment interface
   * @{
   */

  EncodingType encoding_type() const final;
  std::optional<CompressedVectorType> compressed_vector_type() const final;

  /**@}*/

 protected:
  const std::shared_ptr<const pmr_vector<uint32_t>> _encoded_values;
  const std::optional<pmr_vector<bool>> _null_values;
  uint8_t _codec_id;
  ChunkOffset _size;
};

}  // namespace opossum