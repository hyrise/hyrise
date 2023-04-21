#include <future>
#include <memory>
#include <thread>
#include "base_test.hpp"

#include <filesystem>
#include "types.hpp"

namespace hyrise {

class BufferManagerTest : public BaseTest {
 public:
  BufferManager create_buffer_manager(const size_t buffer_pool_size) {
    auto config = BufferManager::Config{};
    config.dram_buffer_pool_size = buffer_pool_size;
    config.ssd_path = db_file;
    config.enable_eviction_worker = false;
    config.mode = BufferManagerMode::DramSSD;
    return BufferManager(config);
  }

  std::shared_ptr<SSDRegion> get_ssd_region(const BufferManager& buffer_manager) {
    return buffer_manager._ssd_region;
  }

  void TearDown() override {
    std::filesystem::remove(db_file);
  }

  const std::string db_file = test_data_path + "buffer_manager.data";
};

TEST_F(BufferManagerTest, TestPinAndUnpinPage) {
  // We create a really small buffer manager with a single frame to test pin and unpin
  auto buffer_manager = create_buffer_manager(bytes_for_size_type(PageSizeType::KiB256));
  const auto ptr = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB256));
  const auto page_id = ptr.get_page_id();

  // Pin the page. The next allocation should fail, since there is only a single buffer frame
  // and it has a pinned page
  buffer_manager.pin_page(page_id, PageSizeType::KiB256);
  EXPECT_ANY_THROW(buffer_manager.allocate(512));

  // Unpin the page. And try again. No the allocation works.
  buffer_manager.unpin_page(page_id, false);
  EXPECT_NO_THROW(buffer_manager.allocate(512));
}

TEST_F(BufferManagerTest, TestFlushDirtyPage) {
  auto buffer_manager = create_buffer_manager(bytes_for_size_type(PageSizeType::KiB256));
  auto ssd_region = get_ssd_region(buffer_manager);

  const auto ptr = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB256));
  const auto page_id = ptr.get_page_id();

  // Write some data to the page
  buffer_manager.pin_page(page_id, PageSizeType::KiB256);
  auto page = buffer_manager.get_page(page_id, PageSizeType::KiB256);
  auto char_page = reinterpret_cast<uint8_t*>(page);
  char_page[0] = 1;
  char_page[1] = 2;

  // Unpin the page and mark it as dirty. There should be nothing on the SSD yet.
  buffer_manager.unpin_page(page_id, true);
  alignas(512) std::array<uint8_t, bytes_for_size_type(PageSizeType::KiB256)> read_buffer1;
  ssd_region->read_page(page_id, PageSizeType::KiB256, reinterpret_cast<std::byte*>(read_buffer1.data()));
  EXPECT_NE(memcmp(page, read_buffer1.data(), bytes_for_size_type(PageSizeType::KiB256)), 0)
      << "The page should not have been written to SSD";

  // TODO: Fix

  // Allocate a new page, which should replace the old one and write it to SSD.
  EXPECT_NO_THROW(buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB256)));
  alignas(512) std::array<uint8_t, bytes_for_size_type(PageSizeType::KiB256)> read_buffer2;
  ssd_region->read_page(page_id, PageSizeType::KiB256, reinterpret_cast<std::byte*>(read_buffer2.data()));
  EXPECT_EQ(memcmp(page, read_buffer2.data(), bytes_for_size_type(PageSizeType::KiB256)), 0)
      << "The page should have been written to SSD";
}

TEST_F(BufferManagerTest, TestMultipleAllocateAndDeallocate) {
  auto buffer_manager = create_buffer_manager(bytes_for_size_type(PageSizeType::KiB256));

  auto ptr = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB256));
  EXPECT_EQ(ptr, BufferPtr<void>(PageID{0}, 0, PageSizeType::KiB256));

  // TODO: Test Sizes capcaity etc

  // TODO: If the page is deallocated, the pointer should be set to 0
  EXPECT_NE(ptr.operator->(), nullptr);
  buffer_manager.deallocate(ptr, bytes_for_size_type(PageSizeType::KiB256));
  EXPECT_EQ(ptr.operator->(), nullptr);

  auto ptr2 = buffer_manager.allocate(1024);
  EXPECT_EQ(ptr2, BufferPtr<void>(PageID{1}, 0, PageSizeType::KiB256));

  buffer_manager.deallocate(ptr2, 1024);
  EXPECT_NE(ptr2.operator->(), nullptr);
  buffer_manager.deallocate(ptr2, 1024);
  EXPECT_EQ(ptr2.operator->(), nullptr);
}

TEST_F(BufferManagerTest, TestAllocateDifferentPageSizes) {
  auto buffer_manager = create_buffer_manager(5 * bytes_for_size_type(PageSizeType::KiB256));

  auto ptr8 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB8));
  EXPECT_EQ(ptr8, BufferPtr<void>(PageID{0}, 0, PageSizeType::KiB8));

  auto ptr16 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB16));
  EXPECT_EQ(ptr16, BufferPtr<void>(PageID{1}, 0, PageSizeType::KiB16));

  auto ptr32 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB32));
  EXPECT_EQ(ptr32, BufferPtr<void>(PageID{2}, 0, PageSizeType::KiB32));

  auto ptr64 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB64));
  EXPECT_EQ(ptr64, BufferPtr<void>(PageID{3}, 0, PageSizeType::KiB64));

  auto ptr128 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB128));
  EXPECT_EQ(ptr128, BufferPtr<void>(PageID{4}, 0, PageSizeType::KiB128));

  auto ptr256 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB256));
  EXPECT_EQ(ptr256, BufferPtr<void>(PageID{5}, 0, PageSizeType::KiB256));

  EXPECT_ANY_THROW(buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB256) + 5));
}

TEST_F(BufferManagerTest, TestHandleConcurrentAllocationsAndDeallocations) {
  auto buffer_manager = create_buffer_manager(20 * bytes_for_size_type(PageSizeType::KiB256));

  // Generate Page Sizes
  tbb::concurrent_queue<PageSizeType> page_sizes;
  for (auto i = 0; i < 500; i++) {
    page_sizes.push(static_cast<PageSizeType>(rand() % NUM_PAGE_SIZE_TYPES));
  }
  const auto run = [&]() {
    PageSizeType size_type;
    while (page_sizes.try_pop(size_type)) {
      auto ptr = buffer_manager.allocate(bytes_for_size_type(size_type));
      // ptr.pin();
      auto raw = static_cast<std::byte*>(ptr.get_pointer());
      *raw = (std::byte)51;
      // ptr.unpin(true);
    }
  };

  // Start 100 threads
  const auto num_threads = uint32_t{10};
  std::vector<std::future<void>> thread_futures;
  thread_futures.reserve(num_threads);

  for (auto thread_num = uint32_t{0}; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, run));
  }

  // Wait for completion or timeout (should not occur)
  for (auto& thread_future : thread_futures) {
    // We give this a lot of time, not because we usually need that long for 100 threads to finish, but because
    // sanitizers and other tools like valgrind sometimes bring a high overhead.
    if (thread_future.wait_for(std::chrono::seconds(180)) == std::future_status::timeout) {
      ASSERT_TRUE(false) << "At least one thread got stuck and did not commit.";
    }
    // Retrieve the future so that exceptions stored in its state are thrown
    thread_future.get();
  }
}

TEST_F(BufferManagerTest, TestUnswizzle) {
  // Write some test case to test the unswizzle function
  auto buffer_manager = create_buffer_manager(bytes_for_size_type(PageSizeType::KiB256));

  auto ptr1 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB32));
  auto [frame1, offset1] = buffer_manager.unswizzle(static_cast<char*>(ptr1.get_pointer()) + 30);
  EXPECT_EQ(offset1, 30);
  EXPECT_EQ(frame1->page_id, PageID{0});
  EXPECT_EQ(frame1->size_type, PageSizeType::KiB32);
  EXPECT_EQ(frame1->page_type, PageType::Dram);

  auto ptr2 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB64));
  auto [frame2, offset2] = buffer_manager.unswizzle(static_cast<char*>(ptr2.get_pointer()) + 30);
  EXPECT_EQ(offset2, 30);
  EXPECT_EQ(frame2->page_id, PageID{0});
  EXPECT_EQ(frame2->size_type, PageSizeType::KiB32);
  EXPECT_EQ(frame2->page_type, PageType::Dram);
}

// TODO: TEsts different modes (DRAM, NVM, Hybrid)

}  // namespace hyrise