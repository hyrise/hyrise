#include "base_test.hpp"
#include "storage/buffer/buffer_pool.hpp"

namespace hyrise {

class BufferPoolTest : public BaseTest {
 public:
  void SetUp() override {
    std::filesystem::create_directory(db_path);
    persistence_manager = std::make_shared<PersistenceManager>(db_path);
    region = VolatileRegion::create_mapped_region();
    volatile_regions = VolatileRegion::create_volatile_regions(region);
    buffer_pool =
        create_buffer_pool(bytes_for_size_type(PageSizeType::KiB16), volatile_regions, persistence_manager, NodeID{0});
  }

  void TearDown() override {
    buffer_pool = nullptr;
    VolatileRegion::unmap_region(region);
  }

  bool ensure_free_pages(const uint64_t size) {
    return buffer_pool->ensure_free_pages(size);
  }

  void add_to_eviction_queue(const PageID page_id) {
    buffer_pool->add_to_eviction_queue(page_id);
  }

  template <typename... Args>
  std::unique_ptr<BufferPool> create_buffer_pool(Args&&... args) {
    return std::unique_ptr<BufferPool>(new BufferPool{std::forward<Args>(args)...});
  }

  const std::string db_path = test_data_path + "buffer_manager_data";
  std::shared_ptr<PersistenceManager> persistence_manager;
  std::byte* region;
  std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> volatile_regions;
  std::unique_ptr<BufferPool> buffer_pool;
};

TEST_F(BufferPoolTest, TestEnsureFreePagesInsufficentSpace) {
  buffer_pool->resize(bytes_for_size_type(PageSizeType::KiB16) * 2);
  EXPECT_EQ(buffer_pool->max_bytes(), bytes_for_size_type(PageSizeType::KiB16) * 2);
  EXPECT_EQ(buffer_pool->reserved_bytes(), 0);

  // We can reserve space for two pages, the third fails because there is not enough space and no item to evict
  EXPECT_TRUE(ensure_free_pages(bytes_for_size_type(PageSizeType::KiB16)));
  EXPECT_TRUE(ensure_free_pages(bytes_for_size_type(PageSizeType::KiB16)));
  EXPECT_EQ(buffer_pool->reserved_bytes(), 2 * bytes_for_size_type(PageSizeType::KiB16));
  EXPECT_FALSE(ensure_free_pages(bytes_for_size_type(PageSizeType::KiB16)));

  auto frame1 = volatile_regions[0]->get_frame(PageID{PageSizeType::KiB16, 0});
  frame1->try_lock_exclusive(frame1->state_and_version());
  frame1->unlock_exclusive();
  add_to_eviction_queue(PageID{PageSizeType::KiB16, 0});

  EXPECT_TRUE(ensure_free_pages(bytes_for_size_type(PageSizeType::KiB16)));

  auto frame2 = volatile_regions[0]->get_frame(PageID{PageSizeType::KiB16, 1});
  frame2->try_lock_exclusive(frame2->state_and_version());
  frame2->unlock_exclusive();
  add_to_eviction_queue(PageID{PageSizeType::KiB16, 1});
  EXPECT_TRUE(ensure_free_pages(bytes_for_size_type(PageSizeType::KiB16)));
}

TEST_F(BufferPoolTest, TestPurgeEvictionQueueInInterval) {
  EXPECT_EQ(buffer_pool->num_eviction_queue_adds(), 0);
  EXPECT_EQ(buffer_pool->num_eviction_queue_items_purged(), 0);
  EXPECT_EQ(buffer_pool->num_evictions(), 0);

  auto page_id = PageID{PageSizeType::KiB16, 0};
  auto frame = volatile_regions[0]->get_frame(page_id);
  for (auto i = 0; i < 1024; ++i) {
    frame->try_lock_exclusive(frame->state_and_version());
    frame->unlock_exclusive();
    add_to_eviction_queue(page_id);
  }

  EXPECT_EQ(buffer_pool->num_eviction_queue_adds(), 1024);
  EXPECT_EQ(buffer_pool->num_eviction_queue_items_purged(), 0);
  EXPECT_EQ(buffer_pool->num_evictions(), 0);

  // Purge interval is 1024, so the next add will purge the queue
  frame->try_lock_exclusive(frame->state_and_version());
  frame->unlock_exclusive();
  add_to_eviction_queue(page_id);

  EXPECT_EQ(buffer_pool->num_eviction_queue_adds(), 1025);
  EXPECT_EQ(buffer_pool->num_eviction_queue_items_purged(), 1024);
  EXPECT_EQ(buffer_pool->num_evictions(), 0);
}

TEST_F(BufferPoolTest, TestWriteDirtyFrameOnEviction) {
  // Setup the buffer pool with a single dirty page to evict
  auto page_id = PageID{PageSizeType::KiB16, 0};
  auto frame = volatile_regions[0]->get_frame(page_id);
  frame->try_lock_exclusive(frame->state_and_version());
  frame->set_dirty(true);
  frame->unlock_exclusive();
  add_to_eviction_queue(page_id);

  EXPECT_EQ(persistence_manager->total_bytes_written(), 0);
  EXPECT_TRUE(frame->is_dirty());

  // Force page eviction by allocation new pages
  ensure_free_pages(bytes_for_size_type(PageSizeType::KiB16));
  ensure_free_pages(bytes_for_size_type(PageSizeType::KiB16));
  ensure_free_pages(bytes_for_size_type(PageSizeType::KiB16));

  EXPECT_EQ(persistence_manager->total_bytes_written(), bytes_for_size_type(PageSizeType::KiB16));
  EXPECT_FALSE(frame->is_dirty());
}

TEST_F(BufferPoolTest, TestIgnoreEvictionForLockedFrame) {}

TEST_F(BufferPoolTest, TestMemoryConsumption) {
  // Test that adding pages to the eviction queue increases the memory consumption
  EXPECT_EQ(buffer_pool->memory_consumption(), sizeof(BufferPool));

  add_to_eviction_queue(PageID{PageSizeType::KiB16, 0});
  EXPECT_EQ(buffer_pool->memory_consumption(), sizeof(BufferPool) + 16);

  add_to_eviction_queue(PageID{PageSizeType::KiB16, 1});
  EXPECT_EQ(buffer_pool->memory_consumption(), sizeof(BufferPool) + 2 * 16);

  // Ensure that memory consumption is reduced when pages are evicted
  ensure_free_pages(2 * bytes_for_size_type(PageSizeType::KiB16));
  EXPECT_EQ(buffer_pool->memory_consumption(), sizeof(BufferPool));
}

TEST_F(BufferPoolTest, TestGetNodeId) {
  buffer_pool =
      create_buffer_pool(bytes_for_size_type(PageSizeType::KiB16), volatile_regions, persistence_manager, NodeID{2});
  EXPECT_EQ(buffer_pool->node_id(), NodeID{2});
}

// TODO: Properly test the resize

}  // namespace hyrise