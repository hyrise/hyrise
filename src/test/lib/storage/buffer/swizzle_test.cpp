#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/types.hpp"

namespace hyrise {

TEST(PointerSwizzling, TestSwizzleUnswizzlePointer) {
  auto data = std::make_unique<std::byte[]>(100);
  EXPECT_TRUE(is_swizzled_pointer(reinterpret_cast<std::uintptr_t>(data.get() + 64)));

  auto unswizzled_ptr = unswizzle_pointer(64, data.get());
  EXPECT_FALSE(is_swizzled_pointer(unswizzled_ptr));

  auto swizzled_ptr = swizzle_pointer(64, data.get());
  EXPECT_TRUE(is_swizzled_pointer(swizzled_ptr));
  EXPECT_EQ(swizzled_ptr, reinterpret_cast<std::uintptr_t>(data.get() + 64));
}

}  // namespace hyrise
