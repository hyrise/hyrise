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
static const auto zs_encoder_for_type = std::map<VectorCompressionType, std::shared_ptr<BaseVectorCompressor>>{
    {VectorCompressionType::FixedSizeByteAligned, std::make_shared<FixedSizeByteAlignedCompressor>()},
    {VectorCompressionType::SimdBp128, std::make_shared<SimdBp128Compressor>()}};

std::unique_ptr<BaseVectorCompressor> create_encoder_by_zs_type(VectorCompressionType type) {
  Assert(type != VectorCompressionType::Invalid, "VectorCompressionType must be valid.");

  auto it = zs_encoder_for_type.find(type);
  Assert(it != zs_encoder_for_type.cend(), "All zero suppression types must be in zs_encoder_for_type.");

  const auto& encoder = it->second;
  return encoder->create_new();
}

}  // namespace

std::unique_ptr<BaseCompressedVector> encode_by_zs_type(const pmr_vector<uint32_t>& vector, VectorCompressionType type,
                                                             const PolymorphicAllocator<size_t>& alloc,
                                                             const UncompressedVectorInfo& meta_info) {
  auto encoder = create_encoder_by_zs_type(type);
  return encoder->encode(vector, alloc, meta_info);
}

}  // namespace opossum
