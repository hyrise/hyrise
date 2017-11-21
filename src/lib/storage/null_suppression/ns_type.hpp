#pragma once

#include <cstdint>


namespace opossum {

enum class NsType : uint8_t {
  FixedSize32ByteAligned,  // “uncompressed”
  FixedSize16ByteAligned,
  FixedSize8ByteAligned,
  SimdBp128
};

}  // namespace opossum
