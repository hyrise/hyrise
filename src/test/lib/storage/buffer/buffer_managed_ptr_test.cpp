#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "types.hpp"

namespace hyrise {
  
using PtrInt = BufferManagedPtr<int32_t>;
using PtrFloat= BufferManagedPtr<float>;

class BufferManagedPtrTest : public BaseTest {
};

TEST_F(BufferManagedPtrTest, TestTypesAndConversions) {
  // using PtrConstInt = BufferManagedPtr<const int32_t>;
}

TEST_F(BufferManagedPtrTest, TestArithmetic) {

  auto preIncrementPtr = PtrInt(PageID{0}, 4);
  EXPECT_EQ((++preIncrementPtr).get_offset(), 8);
  EXPECT_EQ(preIncrementPtr.get_offset(), 8);

  auto postIncrementPtr = PtrInt(PageID{0}, 4);
  EXPECT_EQ((postIncrementPtr++).get_offset(), 4);
  EXPECT_EQ(postIncrementPtr.get_offset(), 8);

  auto preDecrementPtr = PtrInt(PageID{0}, 8);
  EXPECT_EQ((++preDecrementPtr).get_offset(), 4);
  EXPECT_EQ(preDecrementPtr.get_offset(), 4);

  EXPECT_EQ(PtrInt(PageID{0}, 8) - 4, PtrInt(PageID{0}, 4));

  EXPECT_EQ(PtrInt(PageID{0}, 8) + 4, PtrInt(PageID{0}, 12));

  auto incrementAssignPtr = PtrInt(PageID{0}, 8);
  incrementAssignPtr += 8;
  EXPECT_EQ((++preDecrementPtr).get_offset(), 16);

  auto decrementAssignPtr = PtrInt(PageID{0}, 12);
  incrementAssignPtr -= 8;
  EXPECT_EQ((++preDecrementPtr).get_offset(), 4);
}

TEST_F(BufferManagedPtrTest, TestComparisons) {
  EXPECT_TRUE(PtrInt(PageID{0}, 8) < PtrInt(PageID{0}, 12));
  EXPECT_FALSE(PtrInt(PageID{0}, 12) < PtrInt(PageID{0}, 8));

  EXPECT_TRUE(PtrInt(PageID{0}, 12) == PtrInt(PageID{0}, 12));

}
}  // namespace hyrise