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

  struct alignas(512) Page {
    std::array<std::byte, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)> data;
  };

  std::vector<std::tuple<PageSizeType, Page>> write_pages = {
      std::make_tuple(PageSizeType::KiB8, Page{}),
      std::make_tuple(PageSizeType::KiB32, Page{}),
      std::make_tuple(PageSizeType::KiB16, Page{}),
  };

  std::get<1>(write_pages[0]).data[0] = std::byte{0x01};
  std::get<1>(write_pages[1]).data[0] = std::byte{0x02};
  std::get<1>(write_pages[2]).data[0] = std::byte{0x03};

  region->write_page(PageID{1}, std::get<0>(write_pages[1]), std::get<1>(write_pages[1]).data.data());
  region->write_page(PageID{0}, std::get<0>(write_pages[0]), std::get<1>(write_pages[0]).data.data());
  region->write_page(PageID{2}, std::get<0>(write_pages[2]), std::get<1>(write_pages[2]).data.data());

  std::vector<std::tuple<PageSizeType, Page>> read_pages = {
      std::make_tuple(PageSizeType::KiB8, Page{}),
      std::make_tuple(PageSizeType::KiB32, Page{}),
      std::make_tuple(PageSizeType::KiB16, Page{}),
  };
  region->read_page(PageID{2}, std::get<0>(read_pages[2]), std::get<1>(read_pages[2]).data.data());
  region->read_page(PageID{0}, std::get<0>(read_pages[0]), std::get<1>(read_pages[0]).data.data());
  region->read_page(PageID{1}, std::get<0>(read_pages[1]), std::get<1>(read_pages[1]).data.data());

  ASSERT_EQ(std::get<1>(read_pages[0]).data, std::get<1>(write_pages[0]).data);
  ASSERT_EQ(std::get<1>(read_pages[1]).data, std::get<1>(write_pages[1]).data);
  ASSERT_EQ(std::get<1>(read_pages[2]).data, std::get<1>(write_pages[2]).data);
}

TEST_F(SSDRegionTest, TestWriteFailsWithUnalignedData) {
  auto region = std::make_unique<SSDRegion>(db_file);
  std::array<std::byte, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)> data;
  EXPECT_ANY_THROW(region->write_page(PageID{1}, PageSizeType::KiB8, data.data()));
}

// TODO: TEst on block device, GTEST_SKIP if no block

TEST_F(SSDRegionTest, TestBackingFileRemovedAfterDestruction) {
  auto region = std::make_unique<SSDRegion>(db_file);
  ASSERT_TRUE(std::filesystem::exists(db_file));
  region = nullptr;
  ASSERT_FALSE(std::filesystem::exists(db_file));
}

}  // namespace hyrise