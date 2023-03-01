#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/ssd_region.hpp"
#include "types.hpp"

namespace hyrise {

class SSDRegionTest : public BaseTest {
 public:
 protected:
  const std::string db_file = test_data_path + "buffer_manager.data";
};

TEST_F(SSDRegionTest, TestWriteAndReadPagesOnRegularFile) {
  auto region = std::make_unique<SSDRegion>(db_file);

  EXPECT_EQ(region->get_device_type(), SSDRegion::DeviceType::REGULAR_FILE);
  ASSERT_TRUE(std::filesystem::exists(db_file));

  std::vector<Page32KiB> write_pages(3);

  region->write_page(PageID{0}, write_pages[0].size_type(), write_pages[0].data());
  region->write_page(PageID{1}, write_pages[1].size_type(), write_pages[1].data());
  region->write_page(PageID{2}, write_pages[2].size_type(), write_pages[2].data());

  std::vector<Page32KiB> read_pages(3);

  region->read_page(PageID{2}, read_pages[0].size_type(), read_pages[2].data());
  region->read_page(PageID{0}, read_pages[1].size_type(), read_pages[1].data());
  region->read_page(PageID{1}, read_pages[2].size_type(), read_pages[0].data());

  ASSERT_EQ(read_pages[0], write_pages[0]);
  ASSERT_EQ(read_pages[1], write_pages[1]);
  ASSERT_EQ(read_pages[2], write_pages[2]);

  region = nullptr;
  ASSERT_FALSE(std::filesystem::exists(db_file));
}

// TODO: TEst on block device, GTEST_SKIP if no block

// TEST_F(SSDRegionTest, TestBackingFileRemovedAfterDestruction) {
//   auto region = std::make_unique<SSDRegion>(db_file);
//   ASSERT_TRUE(std::filesystem::exists(db_file));
//   region = nullptr;
//   ASSERT_FALSE(std::filesystem::exists(db_file));
// }

}  // namespace hyrise