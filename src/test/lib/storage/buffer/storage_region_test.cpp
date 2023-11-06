#include <memory>

#include "base_test.hpp"

#include <map>

#include <filesystem>

#include "storage/buffer/storage_region.hpp"
#include "types.hpp"
#include "utils/list_directory.hpp"

namespace hyrise {

class StorageRegionTest : public BaseTest {
 public:
  struct alignas(512) Page {
    // We use only pages up to 32 KiB for testing. Larger pages may lead to a stack overflow.
    std::array<std::byte, bytes_for_size_type(PageSizeType::KiB32)> data{};
  };

  void SetUp() override {
    std::filesystem::create_directory(db_path);
    storage_region = std::make_unique<StorageRegion>(db_path);
  }

  const std::string db_path = test_data_path + "buffer_manager_data";
  std::unique_ptr<StorageRegion> storage_region;
};

TEST_F(StorageRegionTest, WriteAndReadPagesOnRegularFile) {
  // Check that one file is created per page size type
  const auto files = list_directory(db_path);
  EXPECT_EQ(files.size(), PAGE_SIZE_TYPES_COUNT) << "Expected one file per page size type";
  EXPECT_EQ(storage_region->mode(), StorageRegion::Mode::FILE_PER_PAGE_SIZE);

  const auto page_ids = std::vector<PageID>{PageID{PageSizeType::KiB16, 20}, PageID{PageSizeType::KiB32, 20},
                                            PageID{PageSizeType::KiB16, 13}};
  auto write_pages = std::vector<Page>{3, Page{}};
  auto read_pages = std::vector<Page>{3, Page{}};

  // Write out 3 different pages
  for (auto index = size_t{0}; index < page_ids.size(); ++index) {
    std::memset(write_pages[index].data.data(), index + 1, page_ids[index].byte_count());
    storage_region->write_page(page_ids[index], write_pages[index].data.data());
  }

  // Read from all pages and check if the data is equal to the written data
  for (auto index = size_t{0}; index < page_ids.size(); ++index) {
    EXPECT_NE(std::memcmp(read_pages[index].data.data(), write_pages[index].data.data(), page_ids[index].byte_count()),
              0);
    storage_region->read_page(page_ids[index], read_pages[index].data.data());
    EXPECT_EQ(std::memcmp(read_pages[index].data.data(), write_pages[index].data.data(), page_ids[index].byte_count()),
              0);
  }

  // Verifiy that the same amount of bytes was written and read
  EXPECT_EQ(storage_region->total_bytes_written(),
            2 * bytes_for_size_type(PageSizeType::KiB16) + bytes_for_size_type(PageSizeType::KiB32));
  EXPECT_EQ(storage_region->total_bytes_read(),
            2 * bytes_for_size_type(PageSizeType::KiB16) + bytes_for_size_type(PageSizeType::KiB32));

  // Check that the files are deleted after the storage region is destroyed
  storage_region = nullptr;
  const auto files_after_cleanup = list_directory(db_path);
  EXPECT_EQ(files_after_cleanup.size(), 0);
}

TEST_F(StorageRegionTest, WriteFailsInvalidPageID) {
  auto page = Page{};
  EXPECT_ANY_THROW(storage_region->write_page(INVALID_PAGE_ID, page.data.data()));
  EXPECT_EQ(storage_region->total_bytes_written(), 0);
}

TEST_F(StorageRegionTest, ReadFailsInvalidPageID) {
  auto page = Page{};
  EXPECT_ANY_THROW(storage_region->read_page(INVALID_PAGE_ID, page.data.data()));
  EXPECT_EQ(storage_region->total_bytes_read(), 0);
}

TEST_F(StorageRegionTest, WriteFailsWithUnalignedData) {
  auto page = Page{};
  EXPECT_ANY_THROW(storage_region->write_page(PageID{PageSizeType::KiB16, 20}, page.data.data() + 5));
  EXPECT_EQ(storage_region->total_bytes_written(), 0);
}

TEST_F(StorageRegionTest, ReadFailsWithUnalignedData) {
  auto page = Page{};
  EXPECT_ANY_THROW(storage_region->read_page(PageID{PageSizeType::KiB16, 0}, page.data.data() + 5));
  EXPECT_EQ(storage_region->total_bytes_read(), 0);
}

TEST_F(StorageRegionTest, WritingTwiceOverridesPreviousData) {
  auto write_page = Page{};
  auto read_page1 = Page{};
  auto page_id = PageID{PageSizeType::KiB16, 20};

  // Perform first write
  std::memset(write_page.data.data(), 0x1, page_id.byte_count());
  storage_region->write_page(page_id, write_page.data.data());
  storage_region->read_page(page_id, read_page1.data.data());
  EXPECT_EQ(std::memcmp(read_page1.data.data(), read_page1.data.data(), page_id.byte_count()), 0);

  // Perform second write and check that the data was overwritten
  auto read_page2 = Page{};
  std::memset(write_page.data.data(), 0x2, page_id.byte_count());
  storage_region->write_page(page_id, write_page.data.data());
  EXPECT_NE(std::memcmp(read_page1.data.data(), read_page2.data.data(), page_id.byte_count()), 0);
}

}  // namespace hyrise
