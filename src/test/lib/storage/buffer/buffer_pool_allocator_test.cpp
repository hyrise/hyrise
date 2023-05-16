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

// TEST_F(BufferPoolAllocatorTest, TestAllocateAndDeallocateIntVector) {
//   // BufferPoolResource uses the global Hyrise Buffer Manager
//   auto allocator = BufferPoolAllocator<size_t>();

//   auto data = std::make_unique<boost::container::vector<int, BufferPoolAllocator<int>>>(5, allocator);
//   data->operator[](0) = 1;
//   data->operator[](1) = 2;
//   data->operator[](2) = 3;
//   data->operator[](3) = 4;
//   data->operator[](4) = 5;

//   auto frame = data->begin().get_ptr().get_frame();
//   auto raw_data = reinterpret_cast<int*>(frame->data);
//   EXPECT_EQ(raw_data[0], 1);
//   EXPECT_EQ(raw_data[1], 2);
//   EXPECT_EQ(raw_data[2], 3);
//   EXPECT_EQ(raw_data[3], 4);
//   EXPECT_EQ(raw_data[4], 5);

//   data = nullptr;
//   // TODO: Test deallocation
// }

// TEST_F(BufferPoolAllocatorTest, TestPolymorphism) {
//   auto int_allocator = BufferPoolAllocator<int>();
//   BufferPoolAllocator<float> other_allocator = int_allocator;
//   auto ptr = other_allocator.allocate(1);
//   static_assert(std::is_same_v<decltype(ptr)::value_type, float>, "Ptr should be of type float");
//   EXPECT_EQ(ptr.get_offset(), 0);
// }

TEST_F(BufferPoolAllocatorTest, TestAllocateAndDeallocateStringVector) {
  auto monotonic_memory_resource = MonotonicBufferResource();
  auto allocator = BufferPoolAllocator<size_t>(&monotonic_memory_resource);

  auto vector = pmr_vector<pmr_string>(140, allocator);
  auto pin_guard = WritePinGuard(vector);
  vector[0] = "Hello";
  vector[1] = "World";
  vector[2] = "Hallo World with a really long string so that we reach the limit of SSO";

  EXPECT_EQ(vector[0], "Hello");
  EXPECT_EQ(vector[1], "World");
  EXPECT_EQ(vector[2], "Hallo World with a really long string so that we reach the limit of SSO");
}

// TEST_F(BufferPoolAllocatorTest, TestObserver) {
//   struct TestObserver : public BufferPoolAllocatorObserver {
//     void on_allocate(const FramePtr& frame) {
//       allocated_frames.push_back(frame);
//     }

//     void on_deallocate(const FramePtr& frame) {
//       allocated_frames.erase(std::remove(allocated_frames.begin(), allocated_frames.end(), frame),
//                              allocated_frames.end());
//     }

//     std::vector<FramePtr> allocated_frames;
//   };

//   auto observer = std::make_shared<TestObserver>();
//   auto allocator = BufferPoolAllocator<size_t>();
//   allocator.register_observer(observer);

//   EXPECT_EQ(observer->allocated_frames.size(), 0);

//   auto ptr = allocator.allocate(1);
//   ASSERT_EQ(observer->allocated_frames.size(), 1);
//   EXPECT_EQ(observer->allocated_frames[0], ptr.get_frame());
//   // The ref count is always one higher than expected due to to temporary creation of the frame
//   EXPECT_EQ(ptr.get_frame()->_internal_ref_count(), 5) << "The Observer should hold a reference to the frame";

//   allocator.deallocate(ptr, 1);
//   ASSERT_EQ(observer->allocated_frames.size(), 0);
//   EXPECT_EQ(ptr.get_frame()->_internal_ref_count(), 4);
//   observer = nullptr;
//   EXPECT_EQ(ptr.get_frame()->_internal_ref_count(), 4) << "The Observer should hold a reference to the frame";

//   auto ptr2 = allocator.allocate(1);
//   EXPECT_EQ(ptr2.get_frame()->_internal_ref_count(), 4);
// }

// TEST_F(BufferPoolAllocatorTest, TestConstructorWithMemoryResource) {
//   class LogResource : public MemoryResource {
//    public:
//     LogResource() = default;

//     BufferPtr<void> allocate(const std::size_t bytes, const std::size_t alignment) override {
//       auto allocated_dram_frame =
//           make_frame(PageID{allocations.size()}, find_fitting_page_size_type(bytes), PageType::Dram);
//       auto ptr = BufferPtr<void>(allocated_dram_frame, 0, typename BufferPtr<void>::AllocTag{});
//       allocations.emplace_back(ptr, bytes, alignment);
//       return ptr;
//     }

//     void deallocate(BufferPtr<void> p, const std::size_t bytes, const std::size_t alignment) override {
//       allocations.erase(std::remove_if(allocations.begin(), allocations.end(),
//                                        [&](const auto& ptr) { return std::get<0>(ptr) == p; }));
//     }

//     std::vector<std::tuple<BufferPtr<void>, std::size_t, std::size_t>> allocations;
//   };

//   auto test_resource = LogResource{};
//   auto allocator = BufferPoolAllocator<size_t>(&test_resource);

//   EXPECT_EQ(test_resource.allocations.size(), 0);
//   auto ptr = allocator.allocate(16);
//   EXPECT_EQ(test_resource.allocations.size(), 1);
//   EXPECT_EQ(std::get<0>(test_resource.allocations[0]).get_offset(), 0);
//   EXPECT_EQ(std::get<0>(test_resource.allocations[0]).get_frame(), ptr.get_frame());
//   EXPECT_EQ(std::get<1>(test_resource.allocations[0]), 128);
//   EXPECT_EQ(std::get<2>(test_resource.allocations[0]), 8);

//   allocator.deallocate(ptr, 16);
//   EXPECT_EQ(test_resource.allocations.size(), 0);
// }

}  // namespace hyrise