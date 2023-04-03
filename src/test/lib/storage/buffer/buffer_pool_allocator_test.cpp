#include <memory>

#include "base_test.hpp"

#include <boost/container/vector.hpp>
#include <filesystem>
#include "storage/buffer/buffer_pool_allocator.hpp"

namespace hyrise {

class BufferPoolAllocatorTest : public BaseTest {
 public:
  BufferManager create_buffer_manager(const size_t buffer_pool_size) {
    auto config = BufferManager::Config{};
    config.dram_buffer_pool_size = buffer_pool_size;
    config.ssd_path = db_file;
    config.enable_eviction_worker = false;
    config.mode = BufferManagerMode::DramSSD;
    return BufferManager(config);
  }

  int get_pin_count(BufferManager& buffer_manager, const PageID page_id) {
    return buffer_manager.get_pin_count(page_id);
  }

 private:
  const std::string db_file = test_data_path + "buffer_manager.data";
};

TEST_F(BufferPoolAllocatorTest, TestAllocateAndDeallocateVector) {
  // BufferPoolResource uses the global Hyrise Buffer Manager
  auto allocator = BufferPoolAllocator<size_t>();

  auto data = std::make_unique<boost::container::vector<int, BufferPoolAllocator<int>>>(5, allocator);
  data->operator[](0) = 1;
  data->operator[](1) = 2;
  data->operator[](2) = 3;
  data->operator[](3) = 4;
  data->operator[](4) = 5;

  auto page = Hyrise::get().buffer_manager.get_page(PageID{0}, PageSizeType::KiB8);
  ASSERT_TRUE(page != nullptr);
  auto raw_data = reinterpret_cast<int*>(page);
  EXPECT_EQ(raw_data[0], 1);
  EXPECT_EQ(raw_data[1], 2);
  EXPECT_EQ(raw_data[2], 3);
  EXPECT_EQ(raw_data[3], 4);
  EXPECT_EQ(raw_data[4], 5);

  data = nullptr;
  ASSERT_TRUE(Hyrise::get().buffer_manager.get_page(PageID{0}, PageSizeType::KiB8) == nullptr);
}

TEST_F(BufferPoolAllocatorTest, TestPolymorphism) {
  // TODO: resource test
}

TEST_F(BufferPoolAllocatorTest, TestConstructRawPointer) {
  struct Dummy {
    int _value1;
    float _value2;

    Dummy(int value1, float value2) : _value1(value1), _value2(value2) {}

    ~Dummy() {
      _value1 = 2;
      _value2 = 2.0;
    }
  };

  auto allocator = BufferPoolAllocator<size_t>();

  auto dummy = Dummy(0, 0);

  // Test that the constructor is called
  allocator.construct(&dummy, 5, 1.0);
  EXPECT_EQ(dummy._value1, 1);
  EXPECT_EQ(dummy._value2, 1.0);
}

TEST_F(BufferPoolAllocatorTest, TestAllocatorPinGuardWithBufferPoolAllocator) {
  // Note: This mechanism only seems to work properly with the global buffer manager due to conversion between raw pointer etc
  auto& buffer_manager = BufferManager::get_global_buffer_manager();

  auto allocator = BufferPoolAllocator<int>{&buffer_manager};
  auto pin_guard = AllocatorPinGuard{allocator};
  auto vector = boost::container::vector<int, BufferPoolAllocator<int>>{100, allocator};

  // The page id 0 should be pinned
  EXPECT_EQ(vector.begin().get_ptr().get_page_id(), PageID{0});
  EXPECT_EQ(get_pin_count(buffer_manager, PageID{0}), 1);

  // Resize the vector to trigger a new page allocation
  vector.resize(1000);

  //The page id 0 should not be pinned anymore, but page id 1 should be pinned
  EXPECT_EQ(vector.begin().get_ptr().get_page_id(), PageID{1});
  EXPECT_EQ(get_pin_count(buffer_manager, PageID{0}), 0);
  EXPECT_EQ(get_pin_count(buffer_manager, PageID{1}), 1);

  // Trigger unpinning of page id 1
  pin_guard = nullptr;

  // No page should be pinned anymore
  EXPECT_EQ(get_pin_count(buffer_manager, PageID{0}), 0);
  EXPECT_EQ(get_pin_count(buffer_manager, PageID{1}), 0);
}

TEST_F(BufferPoolAllocatorTest, TestAllocatorPinGuardWithPolymorphicAllocator) {
  // Note: This mechanism only seems to work properly with the global buffer manager due to conversion between raw pointer etc
  auto& buffer_manager = BufferManager::get_global_buffer_manager();

  auto allocator = PolymorphicAllocator<int>{&buffer_manager};
  auto pin_guard = AllocatorPinGuard{allocator};
  auto vector = pmr_vector<int>{100, allocator};

  // The page id 0 should be pinned
  EXPECT_EQ(vector.begin().get_ptr().get_page_id(), PageID{0});
  EXPECT_EQ(get_pin_count(buffer_manager, PageID{0}), 1);

  // Resize the vector to trigger a new page allocation
  vector.resize(1000);

  //The page id 0 should not be pinned anymore, but page id 1 should be pinned
  EXPECT_EQ(vector.begin().get_ptr().get_page_id(), PageID{1});
  EXPECT_EQ(get_pin_count(buffer_manager, PageID{0}), 0);
  EXPECT_EQ(get_pin_count(buffer_manager, PageID{1}), 1);

  // Trigger unpinning of page id 1
  pin_guard = nullptr;

  // No page should be pinned anymore
  EXPECT_EQ(get_pin_count(buffer_manager, PageID{0}), 0);
  EXPECT_EQ(get_pin_count(buffer_manager, PageID{1}), 0);
}

// TODO
// TEST_F(BufferPoolAllocatorTest, TestConstructRawPointer) {
//   auto allocator = BufferPoolAllocator<size_t>();

//   auto dummy = Dummy(0, 0);

//   // Test that the constructor is called
//   allocator.construct(&dummy, 5, 1.0);
//   EXPECT_EQ(dummy._value1, 1);
//   EXPECT_EQ(dummy._value2, 1.0);
// }

//   // Test that the destructor is called
//   allocator.destroy(&dummy);
//   EXPECT_EQ(dummy._value1, 2);
//   EXPECT_EQ(dummy._value2, 2.0);
// }

}  // namespace hyrise