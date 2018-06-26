#include "jit_runtime_pointer.hpp"

namespace opossum {

uint64_t dereference_flexible_width_int_pointer(const uint64_t address, const unsigned integer_bit_width) {
  switch (integer_bit_width) {
    case 8:
      return *reinterpret_cast<uint8_t*>(address);
    case 16:
      return *reinterpret_cast<uint16_t*>(address);
    case 32:
      return *reinterpret_cast<uint32_t*>(address);
    case 64:
      return *reinterpret_cast<uint64_t*>(address);
    default:
      Fail("Integer pointer with bit width " + std::to_string(integer_bit_width) + " not supported.");
  }
}

}  // namespace opossum
