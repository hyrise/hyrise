#pragma once

#include <cstdint>

namespace opossum {

/**
 * @brief Implemented zero suppression schemes
 *
 * Zero suppression is also known as null suppression
 */
enum class ZsType : uint8_t { Invalid, FixedSizeByteAligned, SimdBp128 };

}  // namespace opossum
