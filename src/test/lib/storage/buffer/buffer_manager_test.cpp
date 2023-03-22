#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/buffer_manager.hpp"
#include "types.hpp"

namespace hyrise {

class BufferManagerTest : public BaseTest {
 public:
  BufferManager create_buffer_manager(const size_t num_bytes = 1 << 10) {
    return BufferManager(num_bytes, db_file);
  }

  void TearDown() override {
    std::filesystem::remove(db_file);
  }

  const std::string db_file = test_data_path + "buffer_manager.data";
};

TEST_F(BufferManagerTest, TestPinAndUnpinPage) {
  // We create a really small buffer manager with a single frame to test pin and unpin
  auto buffer_manager = create_buffer_manager(bytes_for_size_type(PageSizeType::KiB8));
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
  auto buffer_manager = create_buffer_manager(static_cast<size_t>(PageSizeType::KiB8));
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
  auto page_read = std::array<std::byte, bytes_for_size_type(PageSizeType::KiB8)>{};
  ssd_region.read_page(page_id, PageSizeType::KiB8, page_read.data());
  EXPECT_NE(memcmp(page, &page_read, bytes_for_size_type(PageSizeType::KiB8)), 0)
      << "The page should not have been written to SSD";

  // TODO: Fix

  // Allocate a new page, which should replace the old one and write it to SSD.
  EXPECT_NO_THROW(buffer_manager.allocate(512));
  ssd_region.read_page(page_id, PageSizeType::KiB32, page_read.data());
  EXPECT_EQ(memcmp(page, &page_read, bytes_for_size_type(PageSizeType::KiB8)), 0)
      << "The page should have been written to SSD";
}

TEST_F(BufferManagerTest, TestMultipleAllocateAndDeallocate) {
  auto buffer_manager = create_buffer_manager(bytes_for_size_type(PageSizeType::KiB32));

  auto ptr = buffer_manager.allocate(1024);
  EXPECT_EQ(ptr, BufferManagedPtr<void>(PageID{0}, 0, PageSizeType::KiB8));

  // TODO: If the page is deallocated, the pointer should be set to 0
  EXPECT_NE(ptr.operator->(), nullptr);
  buffer_manager.deallocate(ptr, 1024);
  EXPECT_EQ(ptr.operator->(), nullptr);

  auto ptr2 = buffer_manager.allocate(1024);
  EXPECT_EQ(ptr2, BufferManagedPtr<void>(PageID{1}, 0, PageSizeType::KiB8));

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

}  // namespace hyrise