#include "base_test.hpp"
#include "storage/buffer/page_id.hpp"

namespace hyrise {

class PageIDTest : public BaseTest {};

TEST_F(PageIDTest, TestPageIDForDifferentSizeTypes) {
  const auto small_page = PageID{PageSizeType::KiB16, 29, true};
  EXPECT_EQ(small_page.size_type(), PageSizeType::KiB16);
  EXPECT_EQ(small_page.num_bytes(), 16384);
  EXPECT_TRUE(small_page.valid());
  EXPECT_EQ(small_page.index, 29);

  const auto larger_page = PageID{PageSizeType::KiB256, 59, true};
  EXPECT_EQ(larger_page.size_type(), PageSizeType::KiB256);
  EXPECT_EQ(larger_page.num_bytes(), 262144);
  EXPECT_TRUE(larger_page.valid());
  EXPECT_EQ(larger_page.index, 59);
}

TEST_F(PageIDTest, TestInvalidPageID) {
  EXPECT_FALSE(PageID(PageSizeType::KiB16, 29, false).valid());
  EXPECT_FALSE(INVALID_PAGE_ID.valid());
}

}  // namespace hyrise
