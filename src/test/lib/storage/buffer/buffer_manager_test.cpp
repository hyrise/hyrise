#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/buffer_manager.hpp"
#include "types.hpp"

namespace hyrise {

class BufferManagerTest : public BaseTest {
 public:
  BufferManager create_buffer_manager() {
    auto ssd_region = std::make_unique<SSDRegion>(db_file);
    auto volatile_region = std::make_unique<VolatileRegion>(1 << 10);
    return BufferManager(std::move(volatile_region), std::move(ssd_region));
  }

  void TearDown() override {
    std::filesystem::remove(db_file);
  }

 private:
  const std::string db_file = test_data_path + "buffer_manager.data";
};

TEST_F(BufferManagerTest, TestGetPageInVolatileRegion) {
  auto buffer_manager = create_buffer_manager();
  auto page1 = buffer_manager.new_page();
  auto page2 = buffer_manager.new_page();
  auto page3 = buffer_manager.new_page();
}

}  // namespace hyrise