#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include <map>
#include "storage/buffer/persistence_manager.hpp"
#include "types.hpp"

namespace hyrise {

class PersistenceManagerTest : public BaseTest {
 public:
  struct alignas(512) Page {
    // Just use the biggest possible size to avoid different allocations sizes
    std::array<std::byte, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)> data;
  };

  void SetUp() override {
    std::filesystem::create_directory(db_path);
    region = std::make_unique<PersistenceManager>(db_path);
  }

  void files_in_directory(const std::filesystem::path& path, std::vector<std::filesystem::path>& files) {
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      files.push_back(entry.path());
    }
  }

  const std::string db_path = test_data_path + "buffer_manager_data";
  std::unique_ptr<PersistenceManager> region;
};

TEST_F(PersistenceManagerTest, TestWriteAndReadPagesOnRegularFile) {
  auto files = std::vector<std::filesystem::path>{};
  files_in_directory(db_path, files);

  EXPECT_EQ(files.size(), NUM_PAGE_SIZE_TYPES) << "Expected one file per page size type";
  EXPECT_EQ(region->mode(), PersistenceManager::Mode::FILE_PER_SIZE_TYPE);

  auto write_pages = std::map<PageID, Page>{{PageID{PageSizeType::KiB16, 20}, Page{{std::byte{0x11}}}},
                                            {PageID{PageSizeType::KiB32, 20}, Page{{std::byte{0x22}}}},
                                            {PageID{PageSizeType::KiB16, 13}, Page{{std::byte{0x33}}}}};

  for (auto& [page_id, page] : write_pages) {
    // Copy oper the first byte to the whole page
    std::memset(page.data.data(), std::to_integer<int>(*page.data.data()), page_id.num_bytes());
    region->write_page(page_id, page.data.data());
  }

  auto read_pages = std::map<PageID, Page>{{PageID{PageSizeType::KiB16, 20}, Page{{}}},
                                           {PageID{PageSizeType::KiB32, 20}, Page{{}}},
                                           {PageID{PageSizeType::KiB16, 13}, Page{{}}}};
  for (auto& [page_id, page] : write_pages) {
    EXPECT_FALSE(std::memcmp(page.data.data(), read_pages[page_id].data.data(), page_id.num_bytes()));
  }

  for (auto& [page_id, page] : read_pages) {
    region->read_page(page_id, page.data.data());
  }

  for (auto& [page_id, page] : write_pages) {
    EXPECT_TRUE(std::memcmp(page.data.data(), read_pages[page_id].data.data(), page_id.num_bytes()));
  }

  EXPECT_EQ(region->total_bytes_written(),
            2 * bytes_for_size_type(PageSizeType::KiB16) + bytes_for_size_type(PageSizeType::KiB32));
  EXPECT_EQ(region->total_bytes_read(),
            2 * bytes_for_size_type(PageSizeType::KiB16) + bytes_for_size_type(PageSizeType::KiB32));

  region = nullptr;
  auto files_after_cleanup = std::vector<std::filesystem::path>{};
  files_in_directory(db_path, files_after_cleanup);
  EXPECT_EQ(files_after_cleanup.size(), 0);
}

TEST_F(PersistenceManagerTest, TestWriteFailsInvalidPageID) {
  auto page = Page{};
  EXPECT_ANY_THROW(region->write_page(INVALID_PAGE_ID, page.data.data()));
  EXPECT_EQ(region->total_bytes_written(), 0);
}

TEST_F(PersistenceManagerTest, TestReadFailsInvalidPageID) {
  auto page = Page{};
  EXPECT_ANY_THROW(region->read_page(INVALID_PAGE_ID, page.data.data()));
  EXPECT_EQ(region->total_bytes_read(), 0);
}

TEST_F(PersistenceManagerTest, TestWriteFailsWithUnalignedData) {
  auto page = Page{};
  EXPECT_ANY_THROW(region->write_page(PageID{PageSizeType::KiB16, 20}, page.data.data() + 5));
  EXPECT_EQ(region->total_bytes_written(), 0);
}

TEST_F(PersistenceManagerTest, TestReadFailsWithUnalignedData) {
  auto page = Page{};
  EXPECT_ANY_THROW(region->read_page(PageID{PageSizeType::KiB16, 0}, page.data.data() + 5));
  EXPECT_EQ(region->total_bytes_read(), 0);
}

}  // namespace hyrise