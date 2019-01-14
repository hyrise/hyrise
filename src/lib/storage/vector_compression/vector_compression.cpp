#include "vector_compression.hpp"

#include <map> // NEEDEDINCLUDE

#include "base_compressed_vector.hpp"
#include "fixed_size_byte_aligned/fixed_size_byte_aligned_compressor.hpp" // NEEDEDINCLUDE
#include "simd_bp128/simd_bp128_compressor.hpp" // NEEDEDINCLUDE

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

std::unique_ptr<BaseVectorCompressor> create_compressor_by_type(VectorCompressionType type) {
  auto it = vector_compressor_for_type.find(type);
  Assert(it != vector_compressor_for_type.cend(),
         "All vector compression types must be in vector_compressor_for_type.");

  const auto& compressor = it->second;
  return compressor->create_new();
}

}  // namespace

std::unique_ptr<const BaseCompressedVector> compress_vector(const pmr_vector<uint32_t>& vector,
                                                            const VectorCompressionType type,
                                                            const PolymorphicAllocator<size_t>& alloc,
                                                            const UncompressedVectorInfo& meta_info) {
  auto compressor = create_compressor_by_type(type);
  return compressor->compress(vector, alloc, meta_info);
}

}  // namespace opossum
