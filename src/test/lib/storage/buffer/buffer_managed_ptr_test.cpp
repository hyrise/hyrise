#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "gtest_printers.hpp"
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "storage/buffer/buffer_manager.hpp"
#include "types.hpp"

namespace hyrise {

class BufferManagedPtrTest : public BaseTest {
 public:
  using PtrInt = BufferManagedPtr<int32_t>;
  using PtrFloat = BufferManagedPtr<float>;
};

TEST_F(BufferManagedPtrTest, TestSize) {
  static_assert(sizeof(PtrInt) == 16);
  static_assert(sizeof(PtrFloat) == 16);
}

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

  EXPECT_EQ(PtrInt(PageID{0}, 8) - 1, PtrInt(PageID{0}, 4));
  EXPECT_EQ(PtrInt(PageID{0}, 8) + 4, PtrInt(PageID{0}, 24));

  auto incrementAssignPtr = PtrInt(PageID{0}, 8);
  incrementAssignPtr += 3;
  EXPECT_EQ((incrementAssignPtr).get_offset(), 20);

  auto decrementAssignPtr = PtrInt(PageID{0}, 20);
  decrementAssignPtr -= 2;
  EXPECT_EQ((decrementAssignPtr).get_offset(), 12);
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

TEST_F(BufferManagedPtrTest, TestPinGuard) {
  auto ptr = BufferManager::get_global_buffer_manager().allocate(1024);
  auto page_id = ptr.get_page_id();

  // Test PinGuard with non-dirty flag
  {
    EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 0);
    auto pin_guard = PinGuard(ptr, false);
    EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 1);
  }
  EXPECT_FALSE(BufferManager::get_global_buffer_manager().is_dirty(page_id));
  EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 0);

  // Test PinGuard with dirty flag
  {
    EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 0);
    auto pin_guard = PinGuard(ptr, true);
    EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 1);
  }
  EXPECT_TRUE(BufferManager::get_global_buffer_manager().is_dirty(page_id));
  EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 0);
}

TEST_F(BufferManagedPtrTest, TestGetPageIDAndOffset) {
  // TODO: Get Page should nor create a new page
  auto ptr = PtrInt(PageID{5}, 12);

  EXPECT_EQ(ptr.get_page_id(), PageID{5});
  EXPECT_EQ(ptr.get_offset(), 12);
}

// TODO: Work with outside ptr

TEST_F(BufferManagedPtrTest, TestPointerTraits) {
  // TODO: static_assert(std::is_same<std::pointer_traits<PtrInt>::pointer, int32_t*>::value, typeid(std::pointer_traits<PtrInt>::pointer).name());
  static_assert(std::is_same<std::pointer_traits<PtrInt>::element_type, int32_t>::value);
  static_assert(std::is_same<std::pointer_traits<PtrInt>::difference_type, std::ptrdiff_t>::value);
  static_assert(std::is_same<std::pointer_traits<PtrInt>::rebind<float>, PtrFloat>::value);

  // TODO_ TEst with inside and outside address
  auto ptr = PtrInt(PageID{5}, 12);
  EXPECT_EQ(std::pointer_traits<PtrInt>::pointer_to(*ptr), ptr);
}

}  // namespace hyrise