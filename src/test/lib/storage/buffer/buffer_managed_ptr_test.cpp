#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "types.hpp"

namespace hyrise {
class BufferManagedPtrTest : public BaseTest {
  BufferManager& get_global_buffer_manager() {
    return Hyrise::get().buffer_manager;
  }
};

TEST_F(BufferManagedPtrTest, TestTypesAndConversions) {
  // using PtrConstInt = BufferManagedPtr<const int32_t>;
}

TEST_F(BufferManagedPtrTest, TestArithmetic) {
  using PtrInt = BufferManagedPtr<int32_t>;

  auto preIncrementPtr = PtrInt(PageID{0}, 4);
  ++preIncrementPtr;
  EXPECT_EQ(preIncrementPtr.get_offset(), 8);

  auto postIncrementPtr = PtrInt(PageID{0}, 4);
  postIncrementPtr++;
  EXPECT_EQ(postIncrementPtr.get_offset(), 8);
}

TEST_F(BufferManagedPtrTest, TestComparisons) {}
}  // namespace hyrise