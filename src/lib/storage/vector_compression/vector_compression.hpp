#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include "base_compressed_vector.hpp"

#include "types.hpp"

namespace opossum {

/**
 * @brief Implemented vector compression schemes
 *
 * Also known as null suppression and
 * zero suppression in the literature.
 */
enum class VectorCompressionType : uint8_t { Invalid, FixedSizeByteAligned, SimdBp128 };

/**
 * @brief Meta information about an uncompressed vector
 *
 * Some compressors can utilize additional information
 * about the vector which is to be compressed.
 */
struct UncompressedVectorInfo final {
  std::optional<uint32_t> max_value = std::nullopt;
};

/**
 * @brief Compresses a vector of uint32_t using a given VectorCompressionType
 *
 * @param meta_info optional struct that provides the compression algorithms with additional information
 */
std::unique_ptr<const BaseCompressedVector> compress_vector(const pmr_vector<uint32_t>& vector,
                                                            const VectorCompressionType type,
                                                            const PolymorphicAllocator<size_t>& alloc,
                                                            const UncompressedVectorInfo& meta_info = {});

}  // namespace opossum
