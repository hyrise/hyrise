#include <memory>

#include "base_test.hpp"

#include "storage/buffer/memory_resource.hpp"
#include "types.hpp"

namespace hyrise {

class TestResource : public MemoryResource {
 public:
  TestResource() = default;

  BufferPtr<void> allocate(const std::size_t bytes, const std::size_t alignment) override {
    auto buffer = std::get<0>(_allocations.emplace_back(std::make_shared<std::byte[]>(bytes), bytes, alignment));
    return BufferPtr<void>(buffer.get());
  }

  void deallocate(BufferPtr<void> p, const std::size_t bytes, const std::size_t alignment) override {
    Fail("This should not be called");
  }

  std::vector<std::tuple<std::shared_ptr<std::byte[]>, std::size_t, std::size_t>> _allocations;
};

class MonotonicBufferResourceTest : public BaseTest {
 public:
};

TEST_F(MonotonicBufferResourceTest, TestConstructor) {}

TEST_F(MonotonicBufferResourceTest, TestAllocate) {
  auto remaining_storage = std::size_t{0};

  auto test_resource = TestResource{};
  auto monotonic_buffer_resource = MonotonicBufferResource{&test_resource};
  // Test with no buffer
  {
    monotonic_buffer_resource.allocate(1, 1);
    EXPECT_EQ(test_resource._allocations.size(), 1);

  }  // Ask for more internal storage with misaligned current buffer
  {

  }  // request the same alignment to test no storage is wasted
  {

  }  //Exhaust the remaining storage with 2 byte alignment (the last allocation
  //was 4 bytes with 4 byte alignment) so it should be already 2-byte aligned.
  {

  }  //The next allocation should trigger the upstream resource, even with a 1 byte
  //allocation.
  {

  }  //Now try a bigger than next allocation and see if next_buffer_size is doubled.
  {}
}

// TODO: Test 80% fill ratio

TEST_F(MonotonicBufferResourceTest, TestDeallocate) {}

}  // namespace hyrise