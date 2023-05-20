#include <algorithm>
#include <filesystem>
#include <memory>
#include <numeric>
#include <random>
#include "base_test.hpp"
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
    config.enable_eviction_purge_worker = false;
    return BufferManager(config);
  }

  const std::string db_file = test_data_path + "buffer_manager.data";

  FramePtr create_frame(const PageID page_id, PageSizeType size_type) {
    return make_frame(page_id, PageSizeType::KiB8, PageType::Dram);
  }
};

TEST_F(BufferPtrTest, TestSize) {
  static_assert(sizeof(PtrInt) == 16);
  static_assert(sizeof(PtrFloat) == 16);
}

// TODO: test from frame to frame

TEST_F(BufferPtrTest, TestTypesAndConversions) {
  // Test type conversion
  auto type_frame = create_frame(PageID{4}, PageSizeType::KiB8);
  EXPECT_EQ(PtrInt(PtrFloat(type_frame.get(), 8, typename PtrFloat::AllocTag{})),
            PtrInt(type_frame.get(), 8, typename PtrInt::AllocTag{}));

  // Test nullptr
  auto nullPtr = PtrInt(nullptr);
  EXPECT_FALSE(nullPtr);
  EXPECT_TRUE(!nullPtr);
  EXPECT_EQ(nullPtr.operator->(), nullptr);

  // Test Outside address
  auto outsidePtr = PtrInt((int*)0x1);
  EXPECT_TRUE(outsidePtr);
  EXPECT_FALSE(!outsidePtr);
  EXPECT_EQ(outsidePtr.get_frame(), nullptr);
  EXPECT_EQ(outsidePtr.get_offset(), 0x1);
  EXPECT_EQ(outsidePtr.operator->(), (int*)0x1);

  // Test Address in Buffer Manager
  auto alloc_frame = create_frame(PageID{4}, PageSizeType::KiB8);
  auto allocatedPtr = PtrInt(alloc_frame.get(), 30, typename PtrInt::AllocTag{});
  EXPECT_EQ(allocatedPtr.get_frame(), alloc_frame);
  EXPECT_EQ(allocatedPtr.get_offset(), 30);
  EXPECT_NE(allocatedPtr.operator->(), nullptr);

  // TODO: Test for some more properties, deref
}

TEST_F(BufferPtrTest, TestArithmetic) {
  auto preIncrementPtr = PtrInt((void*)0x04);
  EXPECT_EQ((++preIncrementPtr).get_offset(), 8);
  EXPECT_EQ(preIncrementPtr.get_offset(), 8);

  auto postIncrementPtr = PtrInt((void*)0x04);
  EXPECT_EQ((postIncrementPtr++).get_offset(), 4);
  EXPECT_EQ(postIncrementPtr.get_offset(), 8);

  auto preDecrementPtr = PtrInt((void*)0x08);
  EXPECT_EQ((++preDecrementPtr).get_offset(), 12);
  EXPECT_EQ(preDecrementPtr.get_offset(), 12);

  EXPECT_EQ(PtrInt((void*)8) - 1, PtrInt((void*)4));
  EXPECT_EQ(PtrInt((void*)8) + 4, PtrInt((void*)24));

  auto incrementAssignPtr = PtrInt((void*)0x08);
  incrementAssignPtr += 3;
  EXPECT_EQ((incrementAssignPtr).get_offset(), 20);

  auto decrementAssignPtr = PtrInt((void*)20);
  decrementAssignPtr -= 2;
  EXPECT_EQ((decrementAssignPtr).get_offset(), 12);
}

TEST_F(BufferPtrTest, TestComparisons) {
  //   EXPECT_TRUE(PtrInt(create_frame(PageID{4}, PageSizeType::KiB8).get(), 8) <
  //               PtrInt(create_frame(PageID{4}, PageSizeType::KiB8).get(), 12));
  //   EXPECT_FALSE(PtrInt(create_frame(PageID{4}, PageSizeType::KiB8).get(), 12) <
  //                PtrInt(create_frame(PageID{4}, PageSizeType::KiB8).get(), 8));
  //   EXPECT_TRUE(PtrInt(create_frame(PageID{4}, PageSizeType::KiB8).get(), 12) >
  //               PtrInt(create_frame(PageID{4}, PageSizeType::KiB8).get(), 8));
  //   EXPECT_FALSE(PtrInt(create_frame(PageID{4}, PageSizeType::KiB8).get(), 8) >
  //                PtrInt(create_frame(PageID{4}, PageSizeType::KiB8.get(), 12));

  //   EXPECT_TRUE(PtrInt(PageID{0}, 12, PageSizeType::KiB8) == PtrInt(PageID{0}, 12, PageSizeType::KiB8));
  //   EXPECT_NE(PtrInt(PtrFloat(PageID{3}, PageSizeType::KiB128, 8)), PtrInt(PageID{2}, PageSizeType::KiB128, 8));
  //   EXPECT_NE(PtrInt(PtrFloat(PageID{2}, PageSizeType::KiB64, 8)), PtrInt(PageID{2}, PageSizeType::KiB128, 8));
  //   EXPECT_NE(PtrInt(PtrFloat(PageID{2}, PageSizeType::KiB128, 16)), PtrInt(PageID{2}, PageSizeType::KiB128, 16));
}

TEST_F(BufferPtrTest, TestSwap) {
  auto frame1 = create_frame(PageID{4}, PageSizeType::KiB8);
  auto frame2 = create_frame(PageID{5}, PageSizeType::KiB8);

  auto ptr1 = PtrInt(frame1.get(), 8, typename PtrInt::AllocTag{});
  auto ptr2 = PtrInt(frame2.get(), 12, typename PtrInt::AllocTag{});

  swap(ptr1, ptr2);

  EXPECT_EQ(ptr1.get_frame(), frame2);
  EXPECT_EQ(ptr2.get_frame(), frame1);
  EXPECT_EQ(ptr1.get_offset(), 12);
  EXPECT_EQ(ptr2.get_offset(), 8);
}

TEST_F(BufferPtrTest, TestPointerTraits) {
  static_assert(std::is_same<std::pointer_traits<PtrInt>::element_type, int32_t>::value);
  static_assert(std::is_same<std::pointer_traits<PtrInt>::difference_type, std::ptrdiff_t>::value);
  static_assert(std::is_same<std::pointer_traits<PtrInt>::rebind<float>, PtrFloat>::value);

  {
    auto buffer_manager = create_buffer_manager(1024 * 1024);
    auto inside_ptr = static_cast<PtrInt>(buffer_manager.allocate(4)) + 5;
    auto inside_raw_ptr = inside_ptr.operator->();
    EXPECT_EQ(std::pointer_traits<PtrInt>::pointer_to(*inside_raw_ptr), inside_ptr);
  }

  {
    auto outside_ptr = PtrInt{(void*)0x263};
    auto outside_raw_ptr = outside_ptr.operator->();
    EXPECT_EQ(std::pointer_traits<PtrInt>::pointer_to(*outside_raw_ptr), outside_ptr);
  }
}

TEST_F(BufferPtrTest, TestSortVector) {
  auto alloc = BufferPoolAllocator<int>();
  auto pin_guard = AllocatorPinGuard{alloc};
  pmr_vector<int> vec{10000, alloc};

  auto frame = vec.begin().get_ptr().get_frame();
  EXPECT_TRUE(frame->is_pinned());

  std::iota(vec.begin(), vec.end(), 0);
  std::shuffle(vec.begin(), vec.end(), std::mt19937(std::random_device()()));
  EXPECT_FALSE(std::is_sorted(vec.begin(), vec.end()));

  std::sort(vec.begin(), vec.end());
  EXPECT_TRUE(std::is_sorted(vec.begin(), vec.end()));
}

}  // namespace hyrise