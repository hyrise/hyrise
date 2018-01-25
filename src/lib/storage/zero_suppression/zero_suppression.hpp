#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include "base_zero_suppression_vector.hpp"

#include "types.hpp"

namespace opossum {

/**
 * @brief Implemented zero suppression schemes
 *
 * Zero suppression is also known as null suppression
 */
enum class ZsType : uint8_t { Invalid, FixedSizeByteAligned, SimdBp128 };

/**
 * @brief Meta information about an unencoded vector
 *
 * Some encoders can utilize additional information
 * about the vector which is to be encoded.
 */
struct ZsVectorMetaInfo final {
  std::optional<uint32_t> max_value = std::nullopt;
};

/**
 * @brief Encodes a vector of uint32_t using a given ZsType
 *
 * @param meta_info optional struct that provides the encoding algorithms with additional information
 */
std::unique_ptr<BaseZeroSuppressionVector> encode_by_zs_type(const pmr_vector<uint32_t>& vector, ZsType type,
                                                             const PolymorphicAllocator<size_t>& alloc,
                                                             const ZsVectorMetaInfo& meta_info = {});

}  // namespace opossum
