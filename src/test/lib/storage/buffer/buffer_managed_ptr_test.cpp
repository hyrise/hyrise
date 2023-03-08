#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "gtest_printers.hpp"
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/pin_guard.hpp"
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
  EXPECT_EQ(nullPtr.operator->(), nullptr);

  // Test Outside address
  auto outsidePtr = PtrInt((int*)0x1);
  EXPECT_TRUE(outsidePtr);
  EXPECT_FALSE(!outsidePtr);
  EXPECT_EQ(outsidePtr.get_page_id(), 0);
  EXPECT_EQ(outsidePtr.get_offset(), 0);
  EXPECT_EQ(outsidePtr.operator->(), (int*)0x1);

  // Test Address in Buffer Manager
  auto allocatedPtr = BufferManager::get_global_buffer_manager().allocate(1024);
  EXPECT_TRUE(allocatedPtr);
  EXPECT_FALSE(!allocatedPtr);
  EXPECT_EQ(allocatedPtr.get_page_id(), 0);
  EXPECT_EQ(allocatedPtr.get_offset(), 0);
  EXPECT_NE(allocatedPtr.operator->(), nullptr);

  // TODO: Test for some more properties
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
  auto ptr = BufferManager::get_global_buffer_manager().allocate(1024);
  auto page_id = ptr.get_page_id();

  EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 0);

  ptr.pin();
  ptr.pin();
  ptr.pin();
  EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 3);

  ptr.unpin(false);
  EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 2);
  EXPECT_FALSE(BufferManager::get_global_buffer_manager().is_dirty(page_id));

  ptr.unpin(true);
  ptr.unpin(false);
  EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 0);
  EXPECT_TRUE(BufferManager::get_global_buffer_manager().is_dirty(page_id));
}

TEST_F(BufferManagedPtrTest, TestPinGuardNotDirty) {
  // Test PinGuard with non-dirty flag
  auto ptr = BufferManager::get_global_buffer_manager().allocate(1024);
  auto page_id = ptr.get_page_id();
  {
    EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 0);
    auto pin_guard = PinGuard(ptr, false);
    EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 1);
  }
  EXPECT_FALSE(BufferManager::get_global_buffer_manager().is_dirty(page_id));
  EXPECT_EQ(BufferManager::get_global_buffer_manager().get_pin_count(page_id), 0);
}

TEST_F(BufferManagedPtrTest, TestPinGuardDirty) {
  // Test PinGuard with non-dirty flag
  auto ptr = BufferManager::get_global_buffer_manager().allocate(1024);
  auto page_id = ptr.get_page_id();
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