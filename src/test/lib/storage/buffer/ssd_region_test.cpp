#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/ssd_region.hpp"
#include "types.hpp"

namespace hyrise {

class SSDRegionTest : public BaseTest {
 public:
 private:
  const std::string db_file = test_data_path + "buffer_manager.data";
};

// TEST_F(SSDRegionTest, TestWriteReadPage) {
//   const auto region = SSDRegion(db_file);
//   Page page0;
//   Page page1;
//   Page page2;
//   region.write_page(PageID{0}, page0);
//   region.write_page(PageID{1}, page1);
//   region.write_page(PageID{2}, page2);

//   Page readPage0;
//   Page readPage1;
//   Page readPage2;
//   region.read_page(PageID{0}, readPage0);
//   region.read_page(PageID{1}, readPage1);
//   region.read_page(PageID{2}, readPage1);

//   ASSERT_EQUAL(page0, readPage0);
//   ASSERT_EQUAL(page0, readPage1);
//   ASSERT_EQUAL(page0, readPage2);
// }

// TEST_F(SSDRegionTest, TestBackingFileRemovedAfterDestruction) {
//   auto region = std::make_unique<SSDRegion>(db_file);
//   ASSERT_TRUE(std::filesystem::exists(db_file));
//   region = nullptr;
//   ASSERT_FALSE(std::filesystem::exists(db_file));
// }

}  // namespace hyrise