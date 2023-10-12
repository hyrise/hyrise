#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/persistence_manager.hpp"
#include "types.hpp"

namespace hyrise {

class PersistenceManagerTest : public BaseTest {
 public:
  struct alignas(512) Page {
    // Just use the biggest possible size to avoid different allocations
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

  EXPECT_EQ(files.size(), NUM_PAGE_SIZE_TYPES);
  EXPECT_EQ(region->get_mode(), PersistenceManager::Mode::FILE_PER_SIZE_TYPE);

  auto write_pages = std::map<PageID, Page>{{PageID{PageSizeType::KiB16, 20}, Page{{std::byte{0x11}}}},
                                            {PageID{PageSizeType::KiB32, 20}, Page{{std::byte{0x22}}}},
                                            {PageID{PageSizeType::KiB16, 13}, Page{{std::byte{0x33}}}}};

  for (auto& [page_id, page] : write_pages) {
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

  region = nullptr;
  files_in_directory(db_path, files);
  EXPECT_EQ(files.size(), 0);
}

TEST_F(PersistenceManagerTest, TestWriteFailsInvalidPageID) {
  auto region = std::make_unique<PersistenceManager>(db_path);
  auto page = Page{};
  EXPECT_ANY_THROW(region->write_page(INVALID_PAGE_ID, page.data.data()));
}

TEST_F(PersistenceManagerTest, TestReadFailsInvalidPageID) {
  auto region = std::make_unique<PersistenceManager>(db_path);
  auto page = Page{};
  EXPECT_ANY_THROW(region->read_page(INVALID_PAGE_ID, page.data.data()));
}

TEST_F(PersistenceManagerTest, TestWriteFailsWithUnalignedData) {
  auto region = std::make_unique<PersistenceManager>(db_path);
  auto page = Page{};
  EXPECT_ANY_THROW(region->write_page(PageID{PageSizeType::KiB16, 20}, page.data.data() + 5));
}

TEST_F(PersistenceManagerTest, TestReadFailsWithUnalignedData) {
  auto region = std::make_unique<PersistenceManager>(db_path);
  auto page = Page{};
  EXPECT_ANY_THROW(region->read_page(PageID{PageSizeType::KiB16, 0}, page.data.data() + 5));
}

}  // namespace hyrise