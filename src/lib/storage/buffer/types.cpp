#include "storage/buffer/types.hpp"

namespace hyrise {

bool EvictionItem::can_evict() const {
  if (frame->eviction_timestamp != timestamp) {
    return false;
  }
  return frame->can_evict();
}

constexpr std::uintptr_t SWIZZLE_BIT_POS = sizeof(std::uintptr_t) * 8 - 1;
constexpr std::uintptr_t SWIZZLE_BIT_MASK = 1ull << SWIZZLE_BIT_POS;

bool is_swizzled_pointer(const std::uintptr_t ptr) noexcept {
  return (ptr & SWIZZLE_BIT_MASK) == 0;
}

std::uintptr_t swizzle_pointer(const std::uintptr_t offset, const std::byte* data) noexcept {
  return reinterpret_cast<std::uintptr_t>(data + (offset & ~SWIZZLE_BIT_MASK));
}

std::uintptr_t unswizzle_pointer(const std::uintptr_t ptr, const std::byte* data) noexcept {
  return (ptr - reinterpret_cast<std::uintptr_t>(data)) | SWIZZLE_BIT_MASK;
}

}  // namespace hyrise