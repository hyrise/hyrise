#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "types.hpp"

namespace hyrise {

class BufferManagedPtrTest : public BaseTest {
 public:
  using PtrInt = BufferManagedPtr<int32_t>;
  using PtrFloat = BufferManagedPtr<float>;
};

TEST_F(BufferManagedPtrTest, TestTypesAndConversions) {
  // Test type conversion
  EXPECT_EQ(PtrInt(PtrFloat(PageID{0}, 8)), PtrInt(PageID{0}, 8));

  // Test nullptr
  auto nullPtr = PtrInt(nullptr);
  EXPECT_FALSE(nullPtr);
  EXPECT_TRUE(!nullPtr);

  // Test for an address that is in the buffer manager
  // EXPECT_TRUE(nullPtr);

  // Test for an address outside of buffer manager
  // EXPECT_TRUE(nullPtr);

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
  EXPECT_EQ((++incrementAssignPtr).get_offset(), 16);

  auto decrementAssignPtr = PtrInt(PageID{0}, 12);
  decrementAssignPtr -= 8;
  EXPECT_EQ((++decrementAssignPtr).get_offset(), 4);
}

TEST_F(BufferManagedPtrTest, TestComparisons) {
  EXPECT_TRUE(PtrInt(PageID{0}, 8) < PtrInt(PageID{0}, 12));
  EXPECT_FALSE(PtrInt(PageID{0}, 12) < PtrInt(PageID{0}, 8));

  EXPECT_TRUE(PtrInt(PageID{0}, 12) == PtrInt(PageID{0}, 12));
}

TEST_F(BufferManagedPtrTest, TestPinUnpin) {
  // TODO: Get Page should nor create a new page
  auto ptr = BufferManager::get_global_buffer_manager().allocate(1024);
  ptr.pin();
  ptr.unpin(true);
}

TEST_F(BufferManagedPtrTest, TestGetPageIdAndOffset) {
  // TODO: Get Page should nor create a new page
  auto ptr = PtrInt(PageID{5}, 12);

  EXPECT_EQ(ptr.get_page_id(), PageID{5});
  EXPECT_EQ(ptr.get_offset(), 12);
}

// TODO: Work with outside ptr

TEST_F(BufferManagedPtrTest, TestPointerTraits) {
  //TODO:static_assert(std::is_same<std::pointer_traits<PtrInt>::pointer, int32_t*>::value);
  static_assert(std::is_same<std::pointer_traits<PtrInt>::element_type, int32_t>::value);
  static_assert(std::is_same<std::pointer_traits<PtrInt>::difference_type, std::ptrdiff_t>::value);
  static_assert(std::is_same<std::pointer_traits<PtrInt>::rebind<float>, PtrFloat>::value);
}

}  // namespace hyrise