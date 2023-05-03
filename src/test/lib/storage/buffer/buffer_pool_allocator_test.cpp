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
    config.enable_eviction_purge_worker = false;
    config.mode = BufferManagerMode::DramSSD;
    return BufferManager(config);
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

  auto frame = data->begin().get_ptr().get_shared_frame()->dram_frame;
  auto raw_data = reinterpret_cast<int*>(frame->data);
  EXPECT_EQ(raw_data[0], 1);
  EXPECT_EQ(raw_data[1], 2);
  EXPECT_EQ(raw_data[2], 3);
  EXPECT_EQ(raw_data[3], 4);
  EXPECT_EQ(raw_data[4], 5);

  data = nullptr;
  // TODO: Test deallocation
}

TEST_F(BufferPoolAllocatorTest, TestPolymorphism) {
  auto int_allocator = BufferPoolAllocator<int>();
  BufferPoolAllocator<float> other_allocator = int_allocator;
  auto ptr = other_allocator.allocate(1);
  static_assert(std::is_same_v<decltype(ptr)::value_type, float>, "Ptr should be of type float");
  EXPECT_EQ(ptr.get_offset(), 0);
}

// TEST_F(BufferPoolAllocatorTest, TestConstructRawPointer) {
//   struct Dummy {
//     int _value1;
//     float _value2;

//     Dummy(int value1, float value2) : _value1(value1), _value2(value2) {}

//     ~Dummy() {
//       _value1 = 2;
//       _value2 = 2.0;
//     }
//   };

//   auto allocator = BufferPoolAllocator<size_t>();

//   auto dummy = Dummy(0, 0);

//   // Test that the constructor is called
//   allocator.construct(&dummy, 5, 1.0);
//   EXPECT_EQ(dummy._value1, 1);
//   EXPECT_EQ(dummy._value2, 1.0);
// }

TEST_F(BufferPoolAllocatorTest, TestObserver) {
  struct TestObserver : public BufferPoolAllocatorObserver {
    void on_allocate(const std::shared_ptr<SharedFrame>& frame) {
      allocated_frames.push_back(frame);
    }

    void on_deallocate(const std::shared_ptr<SharedFrame>& frame) {
      allocated_frames.erase(std::remove(allocated_frames.begin(), allocated_frames.end(), frame),
                             allocated_frames.end());
    }

    std::vector<std::shared_ptr<SharedFrame>> allocated_frames;
  };

  auto observer = std::make_shared<TestObserver>();
  auto allocator = BufferPoolAllocator<size_t>();
  allocator.register_observer(observer);

  EXPECT_EQ(observer->allocated_frames.size(), 0);

  auto ptr = allocator.allocate(1);
  ASSERT_EQ(observer->allocated_frames.size(), 1);
  EXPECT_EQ(observer->allocated_frames[0], ptr.get_shared_frame());
  EXPECT_EQ(ptr.get_shared_frame().use_count(), 3) << "The Observer should hold a reference to the frame";

  allocator.deallocate(ptr, 1);
  ASSERT_EQ(observer->allocated_frames.size(), 0);
  EXPECT_EQ(ptr.get_shared_frame().use_count(), 2);
  observer = nullptr;
  EXPECT_EQ(ptr.get_shared_frame().use_count(), 2) << "The Observer should hold a reference to the frame";

  auto ptr2 = allocator.allocate(1);
  EXPECT_EQ(ptr2.get_shared_frame().use_count(), 2);
}

TEST_F(BufferPoolAllocatorTest, TestConstructorWithMemoryResource) {
  class LogResource : public MemoryResource {
   public:
    LogResource() = default;

    BufferPtr<void> allocate(const std::size_t bytes, const std::size_t alignment) override {
      auto allocated_dram_frame =
          std::make_shared<Frame>(PageID{allocations.size()}, find_fitting_page_size_type(bytes), PageType::Dram);
      auto shared_frame = std::make_shared<SharedFrame>(allocated_dram_frame);
      allocations.emplace_back(shared_frame, bytes, alignment);
      return BufferPtr<void>(shared_frame, 0);
    }

    void deallocate(BufferPtr<void> p, const std::size_t bytes, const std::size_t alignment) override {
      Fail("This should never be called");
    }

    std::vector<std::tuple<std::shared_ptr<SharedFrame>, std::size_t, std::size_t>> allocations;
  };

  auto test_resource = LogResource{};
  auto allocator = BufferPoolAllocator<size_t>(&test_resource);

  EXPECT_EQ(test_resource.allocations.size(), 0);
  auto ptr = allocator.allocate(16);
  EXPECT_EQ(test_resource.allocations.size(), 1);
  EXPECT_EQ(std::get<0>(test_resource.allocations[0]), ptr.get_shared_frame());
  EXPECT_EQ(std::get<1>(test_resource.allocations[0]), 128);
  EXPECT_EQ(std::get<2>(test_resource.allocations[0]), 8);

  // TODO: Test deallocation
}

// TODO Test with nested DS like string

}  // namespace hyrise