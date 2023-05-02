#include <memory>

#include "base_test.hpp"

#include "storage/buffer/memory_resource.hpp"
#include "types.hpp"

// Used for testing the NewDeleteMemoryResource
std::size_t allocation_count = 0;

void* operator new[](std::size_t size, std::align_val_t align) {
  ++allocation_count;
  auto ptr = std::aligned_alloc(std::size_t(align), size);
  return ptr;
}

void operator delete[](void* p, std::align_val_t align) noexcept {
  --allocation_count;
  return std::free(p);
}

namespace hyrise {

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

class MonotonicBufferResourceTest : public BaseTest {};

TEST_F(MonotonicBufferResourceTest, TestConstructor) {
  auto log_resource = LogResource{};
  {
    // Test default constructor
    auto monotonic_buffer_resource = MonotonicBufferResource{&log_resource};
    monotonic_buffer_resource.allocate(1, 1);
    EXPECT_EQ(monotonic_buffer_resource.remaining_storage(), bytes_for_size_type(PageSizeType::KiB8) - 1u);
  }
  {
    // Test constructor with a higher page size type
    auto monotonic_buffer_resource = MonotonicBufferResource{&log_resource, PageSizeType::KiB64};
    monotonic_buffer_resource.allocate(1, 1);
    EXPECT_EQ(monotonic_buffer_resource.remaining_storage(), bytes_for_size_type(PageSizeType::KiB64) - 1u);
  }
}

TEST_F(MonotonicBufferResourceTest, TestAllocate) {
  // This test is copied from boost
  auto remaining_storage = std::size_t{0};

  auto log_resource = LogResource{};
  auto monotonic_buffer_resource = MonotonicBufferResource{&log_resource};

  {
    // Initial size with no buffer
    EXPECT_EQ(log_resource.allocations.size(), 0);
    monotonic_buffer_resource.allocate(1, 1);
    EXPECT_EQ(log_resource.allocations.size(), 1);
    auto remaining = monotonic_buffer_resource.remaining_storage();
    EXPECT_EQ(remaining, bytes_for_size_type(PageSizeType::KiB8) - 1u);
    remaining_storage = remaining;
  }
  {
    // Ask for more internal storage with misaligned current buffer
    std::size_t wasted_due_to_alignment;
    monotonic_buffer_resource.remaining_storage(4u, wasted_due_to_alignment);
    EXPECT_EQ(wasted_due_to_alignment, 3u);
    monotonic_buffer_resource.allocate(4, 4);
    EXPECT_EQ(log_resource.allocations.size(), 1u);
    auto remaining = monotonic_buffer_resource.remaining_storage();
    //We wasted some bytes due to alignment plus 4 bytes of real storage
    EXPECT_EQ(remaining, remaining_storage - 4 - wasted_due_to_alignment);
    remaining_storage = remaining;
  }
  {
    // request the same alignment to test no storage is wasted
    std::size_t wasted_due_to_alignment;
    auto remaining = monotonic_buffer_resource.remaining_storage(1u, wasted_due_to_alignment);
    EXPECT_EQ(log_resource.allocations.size(), 1u);
    monotonic_buffer_resource.allocate(4, 4);
    //It should not have allocated
    EXPECT_EQ(log_resource.allocations.size(), 1u);
    remaining = monotonic_buffer_resource.remaining_storage();
    //We wasted no bytes due to alignment plus 4 bytes of real storage
    EXPECT_EQ(remaining, remaining_storage - 4u);
    remaining_storage = remaining;
  }
  {
    //Exhaust the remaining storage with 2 byte alignment (the last allocation
    //was 4 bytes with 4 byte alignment) so it should be already 2-byte aligned.
    // We trigger it twice to avoid the 80% fill threshold
    monotonic_buffer_resource.allocate(remaining_storage / 2, 2);
    monotonic_buffer_resource.allocate(monotonic_buffer_resource.remaining_storage(), 2);
    std::size_t wasted_due_to_alignment;
    std::size_t remaining = monotonic_buffer_resource.remaining_storage(1u, wasted_due_to_alignment);
    EXPECT_EQ(wasted_due_to_alignment, 0u);
    EXPECT_EQ(remaining, 0u);
    //It should not have allocated
    EXPECT_EQ(log_resource.allocations.size(), 1u);
    remaining_storage = 0u;
  }
  {
    //The next allocation should trigger the upstream resource
    monotonic_buffer_resource.allocate(1u, 1u);
    EXPECT_EQ(log_resource.allocations.size(), 2u);
    //The next allocation should be geometrically bigger.
    EXPECT_EQ(std::get<1>(log_resource.allocations[1]),
              2 * bytes_for_size_type(MonotonicBufferResource::INITIAL_PAGE_SIZE_TYPE));
    std::size_t wasted_due_to_alignment;
    //For a 2 byte alignment one byte will be wasted from the previous 1 byte allocation
    std::size_t remaining = monotonic_buffer_resource.remaining_storage(2u, wasted_due_to_alignment);
    EXPECT_EQ(wasted_due_to_alignment, 1u);
    EXPECT_EQ(remaining, std::get<1>(log_resource.allocations[1]) - 1u - wasted_due_to_alignment);
    //It should not have allocated
    remaining_storage = monotonic_buffer_resource.remaining_storage(1u);
  }
  {
    // Try some allocation over 80% of a page size and verify the remaining storage hasn't changed,
    // but another page was allocatee from the upstream resource
    monotonic_buffer_resource.allocate(bytes_for_size_type(PageSizeType::KiB64) * 0.85, 1u);
    EXPECT_EQ(log_resource.allocations.size(), 3u);
    EXPECT_EQ(static_cast<size_t>(bytes_for_size_type(PageSizeType::KiB64) * 0.85),
              std::get<1>(log_resource.allocations[2]));
    EXPECT_EQ(remaining_storage, monotonic_buffer_resource.remaining_storage(1u));
    monotonic_buffer_resource.allocate(bytes_for_size_type(PageSizeType::KiB16) * 0.90, 1u);
    EXPECT_EQ(log_resource.allocations.size(), 4u);
    EXPECT_EQ(static_cast<size_t>(bytes_for_size_type(PageSizeType::KiB16) * 0.9),
              std::get<1>(log_resource.allocations[3]));
    EXPECT_EQ(remaining_storage, monotonic_buffer_resource.remaining_storage(1u));
  }
  {
    //Now try a bigger than next allocation and see if the page size is increased
    // We are actually allocating the whole page size, since we are still under 80% of the next page size
    monotonic_buffer_resource.allocate(bytes_for_size_type(PageSizeType::KiB32) * 0.75, 1u);
    EXPECT_EQ(log_resource.allocations.size(), 5u);
    EXPECT_EQ(static_cast<size_t>(bytes_for_size_type(PageSizeType::KiB32)), std::get<1>(log_resource.allocations[4]));

    monotonic_buffer_resource.allocate(bytes_for_size_type(PageSizeType::KiB64) * 0.72, 1u);
    EXPECT_EQ(log_resource.allocations.size(), 6u);
    EXPECT_EQ(static_cast<size_t>(bytes_for_size_type(PageSizeType::KiB64)), std::get<1>(log_resource.allocations[5]));

    monotonic_buffer_resource.allocate(bytes_for_size_type(PageSizeType::KiB128) * 0.79, 1u);
    EXPECT_EQ(log_resource.allocations.size(), 7u);
    EXPECT_EQ(static_cast<size_t>(bytes_for_size_type(PageSizeType::KiB128)), std::get<1>(log_resource.allocations[6]));

    monotonic_buffer_resource.allocate(bytes_for_size_type(PageSizeType::KiB256) * 0.71, 1u);
    EXPECT_EQ(log_resource.allocations.size(), 8u);
    EXPECT_EQ(static_cast<size_t>(bytes_for_size_type(PageSizeType::KiB256)), std::get<1>(log_resource.allocations[7]));
  }
  {
    // Try some more allocations, until we reach the max upstream page size
    monotonic_buffer_resource.allocate(bytes_for_size_type(PageSizeType::KiB256) * 0.75, 1u);
    EXPECT_EQ(log_resource.allocations.size(), 9u);
    EXPECT_EQ(static_cast<size_t>(bytes_for_size_type(PageSizeType::KiB256)), std::get<1>(log_resource.allocations[8]));

    monotonic_buffer_resource.allocate(bytes_for_size_type(PageSizeType::KiB256) * 0.75, 1u);
    EXPECT_EQ(log_resource.allocations.size(), 10u);
    EXPECT_EQ(static_cast<size_t>(bytes_for_size_type(PageSizeType::KiB256)), std::get<1>(log_resource.allocations[9]));

    EXPECT_ANY_THROW(monotonic_buffer_resource.allocate(bytes_for_size_type(PageSizeType::KiB512) * 0.75, 1u));
    EXPECT_EQ(log_resource.allocations.size(), 10u);
  }
}

TEST_F(MonotonicBufferResourceTest, TestDeallocate) {
  auto log_resource = LogResource{};
  constexpr std::size_t iterations = 10;

  auto monotonic_buffer_resource = MonotonicBufferResource{&log_resource};
  std::array<BufferPtr<void>, iterations> buffers;
  std::array<size_t, iterations> sizes;

  for (auto i = 0; i < iterations; ++i) {
    sizes[i] = monotonic_buffer_resource.remaining_storage() + 1;
    monotonic_buffer_resource.allocate(sizes[i], 1);
    EXPECT_EQ(log_resource.allocations.size(), i + 1);
  }

  // All storage stays the same during deallocation
  std::size_t remaining = monotonic_buffer_resource.remaining_storage();
  for (auto i = 0; i < iterations; ++i) {
    monotonic_buffer_resource.deallocate(buffers[i], sizes[i], 1u);
    EXPECT_EQ(log_resource.allocations.size(), iterations);
    EXPECT_EQ(monotonic_buffer_resource.remaining_storage(), remaining);
  }
}

TEST_F(MonotonicBufferResourceTest, TestDestructor) {
  // Verify that the pages still exist after the destructor is called
  auto log_resource = LogResource{};
  {
    auto monotonic_buffer_resource = MonotonicBufferResource{&log_resource};
    for (auto i = 0; i < 10; ++i) {
      monotonic_buffer_resource.allocate(monotonic_buffer_resource.remaining_storage() + 1, 1);
      EXPECT_EQ(log_resource.allocations.size(), i + 1);
    }
  }
  EXPECT_EQ(log_resource.allocations.size(), 10);
}

class NewDeleteMemoryResourceTest : public BaseTest {};

TEST_F(NewDeleteMemoryResourceTest, TestAllocateAndDeallocate) {
  auto memory_resource = NewDeleteMemoryResource{};
  EXPECT_EQ(allocation_count, 0);

  auto ptr = memory_resource.allocate(64, 8);
  EXPECT_EQ(allocation_count, 1);
  EXPECT_EQ(ptr.get_shared_frame(), nullptr) << "Ptr should not buffer managed";
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(ptr.get()) % 4, 0) << "Ptr should be 4 byte aligned";

  memory_resource.deallocate(ptr, 64, 8);
  EXPECT_EQ(allocation_count, 0);
}

}  // namespace hyrise