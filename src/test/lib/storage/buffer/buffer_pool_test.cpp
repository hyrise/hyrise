#include "base_test.hpp"
#include "storage/buffer/buffer_pool.hpp"

namespace hyrise {

class BufferPoolTest : public BaseTest {
 public:
  void SetUp() override {
    std::filesystem::create_directory(db_path);
    persistence_manager = std::make_shared<PersistenceManager>(db_path);
    region = VolatileRegion::create_mapped_region();
    volatile_regions = VolatileRegion::create_volatile_regions(region);
    buffer_pool = std::unique_ptr<BufferPool>(
        new BufferPool{bytes_for_size_type(PageSizeType::KiB16), volatile_regions, persistence_manager, NodeID{0}});
  }

  void TearDown() override {
    buffer_pool = nullptr;
    VolatileRegion::unmap_region(region);
  }

  const std::string db_path = test_data_path + "buffer_manager_data";
  std::shared_ptr<PersistenceManager> persistence_manager;
  std::byte* region;
  std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> volatile_regions;
  std::unique_ptr<BufferPool> buffer_pool;

  void ensure_free_pages(const PageSizeType size) {
    buffer_pool->ensure_free_pages(size);
  }

  void add_to_eviction_queue(const PageID page_id, Frame* frame) {
    buffer_pool->add_to_eviction_queue(page_id, frame);
  }
};

TEST_F(BufferPoolTest, TestEnsureFreePagesInsufficentSpace) {
  buffer_pool->resize(bytes_for_size_type(PageSizeType::KiB16) * 2);
  EXPECT_EQ(buffer_pool->get_max_bytes(), bytes_for_size_type(PageSizeType::KiB16) * 2);
  EXPECT_EQ(buffer_pool->get_reserved_bytes(), 0);
}

}  // namespace hyrise