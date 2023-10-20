#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include <map>
#include "storage/buffer/persistence_manager.hpp"
#include "types.hpp"
#include "utils/list_directory.hpp"

namespace hyrise {

class PersistenceManagerTest : public BaseTest {
 public:
  struct alignas(512) Page {
    // Just use the biggest possible size to avoid different allocations sizes
    std::array<std::byte, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)> data;
  };

  void SetUp() override {
    std::filesystem::create_directory(db_path);
    persistence_manager = std::make_unique<PersistenceManager>(db_path);
  }

  const std::string db_path = test_data_path + "buffer_manager_data";
  std::unique_ptr<PersistenceManager> persistence_manager;
};

TEST_F(PersistenceManagerTest, TestWriteAndReadPagesOnRegularFile) {
  // Check that one file is created per page size type
  const auto files = list_directory(db_path);
  EXPECT_EQ(files.size(), NUM_PAGE_SIZE_TYPES) << "Expected one file per page size type";
  EXPECT_EQ(persistence_manager->mode(), PersistenceManager::Mode::FILE_PER_SIZE_TYPE);

  // Write out 3 different pages
  auto write_pages = std::map<PageID, Page>{{PageID{PageSizeType::KiB16, 20}, Page{{std::byte{0x11}}}},
                                            {PageID{PageSizeType::KiB32, 20}, Page{{std::byte{0x22}}}},
                                            {PageID{PageSizeType::KiB16, 13}, Page{{std::byte{0x33}}}}};
  EXPECT_EQ(write_pages.size(), 3);
  for (auto& [page_id, page] : write_pages) {
    // Copy over the first byte to the whole page
    std::memset(page.data.data(), std::to_integer<int>(*page.data.data()), page_id.num_bytes());
    persistence_manager->write_page(page_id, page.data.data());
  }

  // Create buffers to read into to check if reading works after writing to the same page id
  auto read_pages = std::map<PageID, Page>{{PageID{PageSizeType::KiB16, 20}, Page{{}}},
                                           {PageID{PageSizeType::KiB32, 20}, Page{{}}},
                                           {PageID{PageSizeType::KiB16, 13}, Page{{}}}};
  EXPECT_EQ(read_pages.size(), 3);
  for (auto& [page_id, page] : write_pages) {
    EXPECT_NE(std::memcmp(page.data.data(), read_pages[page_id].data.data(), page_id.num_bytes()), 0);
  }
  for (auto& [page_id, page] : read_pages) {
    persistence_manager->read_page(page_id, page.data.data());
  }
  for (auto& [page_id, page] : write_pages) {
    EXPECT_EQ(std::memcmp(page.data.data(), read_pages[page_id].data.data(), page_id.num_bytes()), 0);
  }

  // Verifiy that the same amount of bytes was written and read
  EXPECT_EQ(persistence_manager->total_bytes_written(),
            2 * bytes_for_size_type(PageSizeType::KiB16) + bytes_for_size_type(PageSizeType::KiB32));
  EXPECT_EQ(persistence_manager->total_bytes_read(),
            2 * bytes_for_size_type(PageSizeType::KiB16) + bytes_for_size_type(PageSizeType::KiB32));

  // Check the the buffer manager files are deleted after the persistence manager is destroyed
  persistence_manager = nullptr;
  const auto files_after_cleanup = list_directory(db_path);
  EXPECT_EQ(files_after_cleanup.size(), 0);
}

TEST_F(PersistenceManagerTest, TestWriteFailsInvalidPageID) {
  auto page = Page{};
  EXPECT_ANY_THROW(persistence_manager->write_page(INVALID_PAGE_ID, page.data.data()));
  EXPECT_EQ(persistence_manager->total_bytes_written(), 0);
}

TEST_F(PersistenceManagerTest, TestReadFailsInvalidPageID) {
  auto page = Page{};
  EXPECT_ANY_THROW(persistence_manager->read_page(INVALID_PAGE_ID, page.data.data()));
  EXPECT_EQ(persistence_manager->total_bytes_read(), 0);
}

TEST_F(PersistenceManagerTest, TestWriteFailsWithUnalignedData) {
  auto page = Page{};
  EXPECT_ANY_THROW(persistence_manager->write_page(PageID{PageSizeType::KiB16, 20}, page.data.data() + 5));
  EXPECT_EQ(persistence_manager->total_bytes_written(), 0);
}

TEST_F(PersistenceManagerTest, TestReadFailsWithUnalignedData) {
  auto page = Page{};
  EXPECT_ANY_THROW(persistence_manager->read_page(PageID{PageSizeType::KiB16, 0}, page.data.data() + 5));
  EXPECT_EQ(persistence_manager->total_bytes_read(), 0);
}

}  // namespace hyrise
