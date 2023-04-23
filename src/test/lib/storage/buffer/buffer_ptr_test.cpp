#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/buffer_ptr.hpp"
#include "storage/buffer/pin_guard.hpp"
#include "types.hpp"

namespace hyrise {

class BufferPtrTest : public BaseTest {
 public:
  using PtrInt = BufferPtr<int32_t>;
  using PtrFloat = BufferPtr<float>;

  BufferManager create_buffer_manager(const size_t buffer_pool_size) {
    auto config = BufferManager::Config{};
    config.dram_buffer_pool_size = buffer_pool_size;
    config.ssd_path = db_file;
    config.enable_eviction_worker = false;
    return BufferManager(config);
  }

  const std::string db_file = test_data_path + "buffer_manager.data";

  int get_pin_count(BufferManager& buffer_manager, const PageID page_id) {
    return buffer_manager.get_pin_count(page_id);
  }

  bool is_page_dirty(BufferManager& buffer_manager, const PageID page_id) {
    return buffer_manager.is_dirty(page_id);
  }
};

TEST_F(BufferPtrTest, TestSize) {
  static_assert(sizeof(PtrInt) == 24);
  static_assert(sizeof(PtrFloat) == 24);
}

TEST_F(BufferPtrTest, TestTypesAndConversions) {
  // Test type conversion
  EXPECT_EQ(PtrInt(PtrFloat(PageID{2}, 8, PageSizeType::KiB128)), PtrInt(PageID{2}, 8, PageSizeType::KiB128));

  // Test nullptr
  auto nullPtr = PtrInt(nullptr);
  EXPECT_FALSE(nullPtr);
  EXPECT_TRUE(!nullPtr);
  EXPECT_EQ(nullPtr.operator->(), nullptr);

  // Test Outside address
  auto outsidePtr = PtrInt((int*)0x1);
  EXPECT_TRUE(outsidePtr);
  EXPECT_FALSE(!outsidePtr);
  EXPECT_EQ(outsidePtr.get_page_id(), INVALID_PAGE_ID);
  EXPECT_ANY_THROW(outsidePtr.get_offset());
  EXPECT_EQ(outsidePtr.operator->(), (int*)0x1);

  // Test Address in Buffer Manager
  auto allocatedPtr = PtrInt(PageID{6}, 30, PageSizeType::KiB8);
  EXPECT_EQ(allocatedPtr.get_page_id(), 6);
  EXPECT_EQ(allocatedPtr.get_offset(), 30);
  EXPECT_NE(allocatedPtr.operator->(), nullptr);

  // TODO: Test for some more properties
}

TEST_F(BufferPtrTest, TestArithmetic) {
  auto preIncrementPtr = PtrInt(PageID{0}, 4, PageSizeType::KiB8);
  EXPECT_EQ((++preIncrementPtr).get_offset(), 8);
  EXPECT_EQ(preIncrementPtr.get_offset(), 8);

  auto postIncrementPtr = PtrInt(PageID{0}, 4, PageSizeType::KiB8);
  EXPECT_EQ((postIncrementPtr++).get_offset(), 4);
  EXPECT_EQ(postIncrementPtr.get_offset(), 8);

  auto preDecrementPtr = PtrInt(PageID{0}, 8, PageSizeType::KiB8);
  EXPECT_EQ((++preDecrementPtr).get_offset(), 4);
  EXPECT_EQ(preDecrementPtr.get_offset(), 4);

  EXPECT_EQ(PtrInt(PageID{0}, 8, PageSizeType::KiB8) - 1, PtrInt(PageID{0}, 4, PageSizeType::KiB8));
  EXPECT_EQ(PtrInt(PageID{0}, 8, PageSizeType::KiB8) + 4, PtrInt(PageID{0}, 24, PageSizeType::KiB8));

  auto incrementAssignPtr = PtrInt(PageID{0}, 8, PageSizeType::KiB8);
  incrementAssignPtr += 3;
  EXPECT_EQ((incrementAssignPtr).get_offset(), 20);

  auto decrementAssignPtr = PtrInt(PageID{0}, 20, PageSizeType::KiB8);
  decrementAssignPtr -= 2;
  EXPECT_EQ((decrementAssignPtr).get_offset(), 12);
}

TEST_F(BufferPtrTest, TestComparisons) {
  EXPECT_TRUE(PtrInt(PageID{0}, 8, PageSizeType::KiB8) < PtrInt(PageID{0}, 12, PageSizeType::KiB8));
  EXPECT_FALSE(PtrInt(PageID{0}, 12, PageSizeType::KiB8) < PtrInt(PageID{0}, 8, PageSizeType::KiB8));

  EXPECT_TRUE(PtrInt(PageID{0}, 12, PageSizeType::KiB8) == PtrInt(PageID{0}, 12, PageSizeType::KiB8));
  // EXPECT_NE(PtrInt(PtrFloat(PageID{3}, PageSizeType::KiB128, 8)), PtrInt(PageID{2}, PageSizeType::KiB128, 8));
  // EXPECT_NE(PtrInt(PtrFloat(PageID{2}, PageSizeType::KiB64, 8)), PtrInt(PageID{2}, PageSizeType::KiB128, 8));
  // EXPECT_NE(PtrInt(PtrFloat(PageID{2}, PageSizeType::KiB128, 16)), PtrInt(PageID{2}, PageSizeType::KiB128, 16));
}

TEST_F(BufferPtrTest, TestPinUnpin) {
  auto buffer_manager = create_buffer_manager(1024 * 1024);

  auto ptr = buffer_manager.allocate(1024);
  auto page_id = ptr.get_page_id();

  EXPECT_EQ(get_pin_count(buffer_manager, page_id), 0);

  ptr.pin();
  ptr.pin();
  ptr.pin();
  EXPECT_EQ(get_pin_count(buffer_manager, page_id), 3);

  ptr.unpin(false);
  EXPECT_EQ(get_pin_count(buffer_manager, page_id), 2);
  EXPECT_FALSE(is_page_dirty(buffer_manager, page_id));

  ptr.unpin(true);
  ptr.unpin(false);
  EXPECT_EQ(get_pin_count(buffer_manager, page_id), 0);
  EXPECT_TRUE(is_page_dirty(buffer_manager, page_id));
}

TEST_F(BufferPtrTest, TestPinGuardNotDirty) {
  // Test PinGuard with non-dirty flag
  auto buffer_manager = create_buffer_manager(1024 * 1024);
  auto allocator = BufferPoolAllocator<int>(&buffer_manager);
  pmr_vector<int> vec{{1, 2, 3, 4, 5}, allocator};

  auto page_id = vec.begin().get_ptr().get_page_id();
  {
    EXPECT_EQ(get_pin_count(buffer_manager, page_id), 0);
    auto pin_guard = PinGuard(vec, false);
    EXPECT_EQ(get_pin_count(buffer_manager, page_id), 1);
  }
  EXPECT_FALSE(is_page_dirty(buffer_manager, page_id));
  EXPECT_EQ(get_pin_count(buffer_manager, page_id), 0);
}

TEST_F(BufferPtrTest, TestPinGuardDirty) {
  // Test PinGuard with non-dirty flag
  auto buffer_manager = create_buffer_manager(1024 * 1024);
  auto allocator = BufferPoolAllocator<int>(&buffer_manager);
  pmr_vector<int> vec{{1, 2, 3, 4, 5}, allocator};

  auto page_id = vec.begin().get_ptr().get_page_id();
  {
    EXPECT_EQ(get_pin_count(buffer_manager, page_id), 0);
    auto pin_guard = PinGuard(vec, true);
    EXPECT_EQ(get_pin_count(buffer_manager, page_id), 1);
  }
  EXPECT_TRUE(is_page_dirty(buffer_manager, page_id));
  EXPECT_EQ(get_pin_count(buffer_manager, page_id), 0);
}

TEST_F(BufferPtrTest, TestGetPageIDAndOffset) {
  // TODO: Get Page should nor create a new page
  auto ptr = PtrInt(PageID{5}, 12, PageSizeType::KiB16);

  EXPECT_EQ(ptr.get_page_id(), PageID{5});
  EXPECT_EQ(ptr.get_offset(), 12);
  EXPECT_EQ(ptr.get_size_type(), PageSizeType::KiB16);
}

// TODO: Work with outside ptr

TEST_F(BufferPtrTest, TestPointerTraits) {
  // TODO: static_assert(std::is_same<std::pointer_traits<PtrInt>::pointer, int32_t*>::value, typeid(std::pointer_traits<PtrInt>::pointer).name());
  static_assert(std::is_same<std::pointer_traits<PtrInt>::element_type, int32_t>::value);
  static_assert(std::is_same<std::pointer_traits<PtrInt>::difference_type, std::ptrdiff_t>::value);
  static_assert(std::is_same<std::pointer_traits<PtrInt>::rebind<float>, PtrFloat>::value);

  // TODO_ TEst with inside and outside address
  auto buffer_manager = create_buffer_manager(1024 * 1024);
  auto ptr = static_cast<PtrInt>(buffer_manager.allocate(4));
  EXPECT_EQ(std::pointer_traits<PtrInt>::pointer_to(*ptr), ptr);
}

}  // namespace hyrise