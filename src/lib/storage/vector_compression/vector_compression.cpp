#include "vector_compression.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <ostream>

#include "base_compressed_vector.hpp"
#include "base_vector_compressor.hpp"
#include "bitpacking/bitpacking_compressor.hpp"
#include "fixed_width_integer/fixed_width_integer_compressor.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

namespace {

/**
 * @brief Mapping of vector compression types to compressors
 *
 * Add your vector compressor here!
 */
const auto vector_compressor_for_type = std::map<VectorCompressionType, std::shared_ptr<BaseVectorCompressor>>{
    {VectorCompressionType::FixedWidthInteger, std::make_shared<FixedWidthIntegerCompressor>()},
    {VectorCompressionType::BitPacking, std::make_shared<BitPackingCompressor>()}};

std::unique_ptr<BaseVectorCompressor> create_compressor_by_type(VectorCompressionType type) {
  auto iter = vector_compressor_for_type.find(type);
  Assert(iter != vector_compressor_for_type.cend(),
         "All vector compression types must be in vector_compressor_for_type.");

  const auto& compressor = iter->second;
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

std::ostream& operator<<(std::ostream& stream, const VectorCompressionType vector_compression_type) {
  return stream << vector_compression_type_to_string.left.at(vector_compression_type);
}

}  // namespace hyrise
