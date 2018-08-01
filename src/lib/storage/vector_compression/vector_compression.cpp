#include "vector_compression.hpp"

#include <map>
#include <memory>

#include "utils/assert.hpp"

#include "fixed_size_byte_aligned/fixed_size_byte_aligned_compressor.hpp"
#include "simd_bp128/simd_bp128_compressor.hpp"

namespace opossum {

namespace {

/**
 * @brief Mapping of vector compression types to compressors
 *
 * Add your vector compressor here!
 */
const auto vector_compressor_for_type = std::map<VectorCompressionType, std::shared_ptr<BaseVectorCompressor>>{
    {VectorCompressionType::FixedSizeByteAligned, std::make_shared<FixedSizeByteAlignedCompressor>()},
    {VectorCompressionType::SimdBp128, std::make_shared<SimdBp128Compressor>()}};

std::unique_ptr<BaseVectorCompressor> create_encoder_by_type(VectorCompressionType type) {
  Assert(type != VectorCompressionType::Invalid, "VectorCompressionType must be valid.");

  auto it = vector_compressor_for_type.find(type);
  Assert(it != vector_compressor_for_type.cend(),
         "All vector compression types must be in vector_compressor_for_type.");

  const auto& encoder = it->second;
  return encoder->create_new();
}

}  // namespace

std::unique_ptr<const BaseCompressedVector> compress_vector(const pmr_vector<uint32_t>& vector,
                                                            const VectorCompressionType type,
                                                            const PolymorphicAllocator<size_t>& alloc,
                                                            const UncompressedVectorInfo& meta_info) {
  auto encoder = create_encoder_by_type(type);
  return encoder->encode(vector, alloc, meta_info);
}

}  // namespace opossum
