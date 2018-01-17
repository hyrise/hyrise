#pragma once

#include <cstdint>
#include <optional>

namespace opossum {

/**
 * @brief Meta information about an unencoded vector
 *
 * Some encoders can utilize additional information
 * about the vector which is to be encoded.
 */
struct ZsVectorMetaInfo {
  std::optional<uint32_t> max_value = std::nullopt;
};

}  // namespace opossum
