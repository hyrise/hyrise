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
    // Just use the biggest possible size to avoid different allocations
    std::array<std::byte, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)> data;
  };

  auto page_data = std::vector<Page>{
      Page{},
      Page{},
      Page{},
  };

  auto write_frames = std::vector<std::shared_ptr<Frame>>{
      std::make_shared<Frame>(PageID{0}, PageSizeType::KiB8, PageType::Dram, page_data[0].data.data()),
      std::make_shared<Frame>(PageID{1}, PageSizeType::KiB32, PageType::Dram, page_data[1].data.data()),
      std::make_shared<Frame>(PageID{2}, PageSizeType::KiB16, PageType::Dram, page_data[2].data.data()),
  };

  write_frames[0]->data[0] = std::byte{0x01};
  write_frames[1]->data[0] = std::byte{0x02};
  write_frames[2]->data[0] = std::byte{0x03};

  region->write_page(write_frames[0]);
  region->write_page(write_frames[3]);
  region->write_page(write_frames[1]);

  auto read_data = std::vector<Page>{
      Page{},
      Page{},
      Page{},
  };

  auto read_frames = std::vector<std::shared_ptr<Frame>>{
      std::make_shared<Frame>(PageID{1}, PageSizeType::KiB32, PageType::Dram, read_data[1].data.data()),
      std::make_shared<Frame>(PageID{2}, PageSizeType::KiB16, PageType::Dram, read_data[2].data.data()),
      std::make_shared<Frame>(PageID{0}, PageSizeType::KiB8, PageType::Dram, read_data[0].data.data()),
  };
  region->read_page(read_frames[0]);
  region->read_page(read_frames[1]);
  region->read_page(read_frames[2]);

  // TODO; memcpy
  // ASSERT_TRUE(std::get<1>(read_pages[0]).data, std::get<1>(write_pages[0]).data);
  // ASSERT_EQ(std::get<1>(read_pages[1]).data, std::get<1>(write_pages[1]).data);
  // ASSERT_EQ(std::get<1>(read_pages[2]).data, std::get<1>(write_pages[2]).data);
}

TEST_F(SSDRegionTest, TestWriteFailsWithUnalignedData) {
  auto region = std::make_unique<SSDRegion>(db_file);
  std::array<std::byte, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)> data;
  auto frame = std::make_shared<Frame>(PageID{0}, PageSizeType::KiB8, PageType::Dram, data.data());
  EXPECT_ANY_THROW(region->write_page(frame));
}

TEST_F(SSDRegionTest, TestReadFailsWithUnalignedData) {
  auto region = std::make_unique<SSDRegion>(db_file);
  std::array<std::byte, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)> data;
  auto frame = std::make_shared<Frame>(PageID{0}, PageSizeType::KiB8, PageType::Dram, data.data());
  EXPECT_ANY_THROW(region->read_page(frame));
}

// TODO: TEst on block device, GTEST_SKIP if no block

TEST_F(SSDRegionTest, TestBackingFileRemovedAfterDestruction) {
  auto region = std::make_unique<SSDRegion>(db_file);
  ASSERT_TRUE(std::filesystem::exists(db_file));
  region = nullptr;
  ASSERT_FALSE(std::filesystem::exists(db_file));
}

}  // namespace hyrise