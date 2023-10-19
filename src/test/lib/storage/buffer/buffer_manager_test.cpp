#include "base_test.hpp"
#include "storage/buffer/allocator.hpp"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

class BufferManagerTest : public BaseTest {
 public:
  void SetUp() override {
    std::filesystem::create_directory(db_path);
    buffer_manager = std::make_unique<BufferManager>(1 << 20, db_path);
  }

  void TearDown() override {
    buffer_manager = nullptr;
  }

  const std::filesystem::path db_path = test_data_path + "buffer_manager_data";
  std::unique_ptr<BufferManager> buffer_manager;
};

TEST_F(BufferManagerTest, TestPinExclusive) {
  const auto& buffer_pool = buffer_manager->buffer_pool();

  EXPECT_EQ(buffer_pool.num_eviction_queue_adds(), 0);

  // TODO: cannot pin exclusive if page is already pinned
  {
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 0}), Frame::EVICTED);
    buffer_manager->pin_exclusive(PageID{PageSizeType::KiB16, 0});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 0}), Frame::LOCKED);
    EXPECT_EQ(buffer_manager->total_hits(), 0);
    EXPECT_EQ(buffer_manager->total_misses(), 1);
    EXPECT_EQ(buffer_manager->total_pins(), 1);
  }
  {
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 1}), Frame::EVICTED);
    buffer_manager->pin_exclusive(PageID{PageSizeType::KiB16, 1});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 1}), Frame::LOCKED);
    EXPECT_EQ(buffer_manager->total_hits(), 0);
    EXPECT_EQ(buffer_manager->total_misses(), 2);
    EXPECT_EQ(buffer_manager->total_pins(), 2);
  }
  {
    EXPECT_ANY_THROW(buffer_manager->pin_exclusive(PageID{PageSizeType::KiB16, 1}))
        << "Cannot pin exclusive twice. This results in a deadlock.";
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 1}), Frame::LOCKED);
    EXPECT_EQ(buffer_manager->total_hits(), 0);
    EXPECT_EQ(buffer_manager->total_misses(), 2);
    EXPECT_EQ(buffer_manager->total_pins(), 3);
  }
  {
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB256, 0}), Frame::EVICTED);
    buffer_manager->pin_exclusive(PageID{PageSizeType::KiB256, 0});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB256, 0}), Frame::LOCKED);
    EXPECT_EQ(buffer_manager->total_hits(), 0);
    EXPECT_EQ(buffer_manager->total_misses(), 3);
    EXPECT_EQ(buffer_manager->total_pins(), 3);
  }
  {
    buffer_manager->unpin_exclusive(PageID{PageSizeType::KiB16, 0});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 0}), Frame::UNLOCKED);
    EXPECT_EQ(buffer_pool.num_eviction_queue_adds(), 1);
    EXPECT_EQ(buffer_manager->total_hits(), 0);
    EXPECT_EQ(buffer_manager->total_misses(), 3);
    EXPECT_EQ(buffer_manager->total_pins(), 2);
  }
  {
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB256, 0}), Frame::UNLOCKED);
    buffer_manager->pin_exclusive(PageID{PageSizeType::KiB16, 0});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB256, 0}), Frame::LOCKED);
    EXPECT_EQ(buffer_manager->total_hits(), 1);
    EXPECT_EQ(buffer_manager->total_misses(), 3);
    EXPECT_EQ(buffer_manager->total_pins(), 3);
  }
  {
    buffer_manager->unpin_exclusive(PageID{PageSizeType::KiB16, 0});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB256, 0}), Frame::UNLOCKED);
    EXPECT_EQ(buffer_pool.num_eviction_queue_adds(), 2);
    EXPECT_EQ(buffer_manager->total_hits(), 1);
    EXPECT_EQ(buffer_manager->total_misses(), 3);
    EXPECT_EQ(buffer_manager->total_pins(), 2);
  }
  {
    buffer_manager->unpin_exclusive(PageID{PageSizeType::KiB16, 1});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 1}), Frame::UNLOCKED);
    EXPECT_EQ(buffer_pool.num_eviction_queue_adds(), 3);
    EXPECT_EQ(buffer_manager->total_hits(), 1);
    EXPECT_EQ(buffer_manager->total_misses(), 3);
    EXPECT_EQ(buffer_manager->total_pins(), 1);
  }
  {
    buffer_manager->unpin_exclusive(PageID{PageSizeType::KiB256, 0});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB256, 0}), Frame::UNLOCKED);
    EXPECT_EQ(buffer_pool.num_eviction_queue_adds(), 4);
    EXPECT_EQ(buffer_manager->total_hits(), 1);
    EXPECT_EQ(buffer_manager->total_misses(), 3);
    EXPECT_EQ(buffer_manager->total_pins(), 0);
  }
}

TEST_F(BufferManagerTest, TestPinShared) {
  {
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 0}), Frame::EVICTED);
    buffer_manager->pin_shared(PageID{PageSizeType::KiB16, 0});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 0}), 1);
    EXPECT_EQ(buffer_manager->total_hits(), 0);
    EXPECT_EQ(buffer_manager->total_misses(), 1);
    EXPECT_EQ(buffer_manager->total_pins(), 1);
  }
  {
    buffer_manager->pin_shared(PageID{PageSizeType::KiB16, 0});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 0}), 2);
    EXPECT_EQ(buffer_manager->total_hits(), 1);
    EXPECT_EQ(buffer_manager->total_misses(), 1);
    EXPECT_EQ(buffer_manager->total_pins(), 2);
  }
  {
    buffer_manager->pin_shared(PageID{PageSizeType::KiB16, 1});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 0}), 1);
    EXPECT_EQ(buffer_manager->total_hits(), 1);
    EXPECT_EQ(buffer_manager->total_misses(), 2);
    EXPECT_EQ(buffer_manager->total_pins(), 3);
  }
  {
    buffer_manager->pin_shared(PageID{PageSizeType::KiB16, 0});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 0}), 3);
    EXPECT_EQ(buffer_manager->total_hits(), 2);
    EXPECT_EQ(buffer_manager->total_misses(), 2);
    EXPECT_EQ(buffer_manager->total_pins(), 4);
  }
  {
    buffer_manager->unpin_shared(PageID{PageSizeType::KiB16, 0});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 0}), 2);
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 0}), 3);
    EXPECT_EQ(buffer_manager->total_hits(), 2);
    EXPECT_EQ(buffer_manager->total_misses(), 2);
    EXPECT_EQ(buffer_manager->total_pins(), 3);
  }
  {
    buffer_manager->unpin_shared(PageID{PageSizeType::KiB16, 0});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 0}), 1);
    EXPECT_EQ(buffer_manager->total_hits(), 2);
    EXPECT_EQ(buffer_manager->total_misses(), 2);
    EXPECT_EQ(buffer_manager->total_pins(), 2);
  }
  {
    buffer_manager->unpin_shared(PageID{PageSizeType::KiB16, 0});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 0}), Frame::UNLOCKED);
    EXPECT_EQ(buffer_manager->total_hits(), 2);
    EXPECT_EQ(buffer_manager->total_misses(), 2);
    EXPECT_EQ(buffer_manager->total_pins(), 1);
  }
  {
    buffer_manager->unpin_shared(PageID{PageSizeType::KiB16, 1});
    EXPECT_EQ(buffer_manager->page_state(PageID{PageSizeType::KiB16, 1}), Frame::UNLOCKED);
    EXPECT_EQ(buffer_manager->total_hits(), 2);
    EXPECT_EQ(buffer_manager->total_misses(), 2);
    EXPECT_EQ(buffer_manager->total_pins(), 0);
  }
}

// Test dirty

TEST_F(BufferManagerTest, TestFindPage) {
  auto page_allocator = PageMemoryResource(buffer_manager.get());

  const auto small_page_ptr_1 = page_allocator.allocate(bytes_for_size_type(PageSizeType::KiB16));
  const auto small_page_ptr_2 = page_allocator.allocate(bytes_for_size_type(PageSizeType::KiB16));
  const auto large_page_ptr = page_allocator.allocate(bytes_for_size_type(PageSizeType::KiB256));
  const auto invalid_page_ptr = reinterpret_cast<void*>(0x1337);

  EXPECT_EQ(buffer_manager->find_page(small_page_ptr_2), (PageID{PageSizeType::KiB16, 1}));
  EXPECT_EQ(buffer_manager->find_page(small_page_ptr_1), (PageID{PageSizeType::KiB16, 0}));
  EXPECT_EQ(buffer_manager->find_page(large_page_ptr), (PageID{PageSizeType::KiB256, 0}));
  EXPECT_EQ(buffer_manager->find_page(invalid_page_ptr), INVALID_PAGE_ID);
}

}  // namespace hyrise