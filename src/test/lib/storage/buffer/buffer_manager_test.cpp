#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/buffer_manager.hpp"
#include "types.hpp"

namespace hyrise {

class BufferManagerTest : public BaseTest {
 public:
  BufferManager create_buffer_manager(const size_t num_bytes = 1 << 10) {
    auto ssd_region = std::make_unique<SSDRegion>(db_file);
    auto volatile_region = std::make_unique<VolatileRegion>(num_bytes);
    return BufferManager(std::move(volatile_region), std::move(ssd_region));
  }

  void TearDown() override {
    std::filesystem::remove(db_file);
  }

  const std::string db_file = test_data_path + "buffer_manager.data";
};

TEST_F(BufferManagerTest, TestPinAndUnpinPage) {
  // We create a really small buffer manager with a single frame to test pin and unpin
  auto buffer_manager = create_buffer_manager(static_cast<size_t>(PageSizeType::KiB32));
  const auto ptr = buffer_manager.allocate(512);
  const auto page_id = ptr.get_page_id();

  // Pin the page. The next allocation should fail, since there is only a single buffer frame
  // and it has a pinned page
  buffer_manager.pin_page(page_id);
  EXPECT_ANY_THROW(buffer_manager.allocate(512));

  // Unpin the page. And try again. No the allocation works.
  buffer_manager.unpin_page(page_id, false);
  EXPECT_NO_THROW(buffer_manager.allocate(512));
}

TEST_F(BufferManagerTest, TestFlushDirtyPage) {
  auto buffer_manager = create_buffer_manager(static_cast<size_t>(PageSizeType::KiB32));
  auto ssd_region = SSDRegion(db_file);
  const auto ptr = buffer_manager.allocate(512);
  const auto page_id = ptr.get_page_id();

  // Write some data to the page
  buffer_manager.pin_page(page_id);
  auto page = buffer_manager.get_page(page_id);
  auto char_page = reinterpret_cast<uint8_t*>(page);
  char_page[0] = 1;
  char_page[1] = 2;

  // Unpin the page and mark it as dirty. There should be nothing on the SSD yet.
  buffer_manager.unpin_page(page_id, true);
  auto page_read = Page32KiB{};
  ssd_region.read_page(page_id, page_read);
  EXPECT_NE(memcmp(page, &page_read, sizeof(Page32KiB)), 0) << "The page should not have been written to SSD";

  // Allocate a new page, which should replace the old one and write it to SSD.
  EXPECT_NO_THROW(buffer_manager.allocate(512));
  ssd_region.read_page(page_id, page_read);
  EXPECT_EQ(memcmp(page, &page_read, sizeof(Page32KiB)), 0) << "The page should have been written to SSD";
}

TEST_F(BufferManagerTest, TestMultipleAllocateAndDeallocate) {
  auto buffer_manager = create_buffer_manager(static_cast<size_t>(PageSizeType::KiB32));
  
  auto ptr = buffer_manager.allocate(1024);
  EXPECT_EQ(ptr, BufferManagedPtr<void>(PageID{0}, 0));

  EXPECT_NE(ptr.operator->(), nullptr);
  buffer_manager.deallocate(ptr, 1024);
  EXPECT_EQ(ptr.operator->(), nullptr);

  auto ptr2 = buffer_manager.allocate(1024);
  EXPECT_EQ(ptr2, BufferManagedPtr<void>(PageID{1}, 0));

  buffer_manager.deallocate(ptr2, 1024);
  EXPECT_NE(ptr2.operator->(), nullptr);
  buffer_manager.deallocate(ptr2, 1024);
  EXPECT_EQ(ptr2.operator->(), nullptr);
}

TEST_F(BufferManagerTest, TestAllocateDifferentSizes) {
  GTEST_SKIP() << "TODO";

  // auto buffer_manager = create_buffer_manager(static_cast<size_t>(PageSizeType::KiB32));
  // auto ptr = buffer_manager.allocate(static_cast<size_t>(PageSizeType::KiB32));
}

// void flush_page(const PageID page_id);
// void remove_page(const PageID page_id);
// void read_page(const PageID page_id, Page32KiB& destination);
// void write_page(const PageID page_id, Page32KiB& source);

}  // namespace hyrise