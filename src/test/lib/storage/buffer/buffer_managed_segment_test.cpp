#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/buffer_managed_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace hyrise {

class BufferManagedSegmentTest : public BaseTest {
 public:
  std::shared_ptr<BufferManager> create_buffer_manager() {
    auto ssd_region = std::make_unique<SSDRegion>(db_file);
    auto volatile_region = std::make_unique<VolatileRegion>(1 << 10);
    return std::make_shared<BufferManager>(std::move(volatile_region), std::move(ssd_region));
  }

  void TearDown() override {
    std::filesystem::remove(db_file);
  }

 private:
  const std::string db_file = test_data_path + "buffer_manager.data";
};

TEST_F(BufferManagedSegmentTest, TestGetPageInVolatileRegion) {
  auto buffer_manager = create_buffer_manager();
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{17, 0, 1, 1},
                                                               pmr_vector<bool>{true, true, false, false});
  auto buffer_managed_segment =
      std::make_shared<BufferManagedSegment<int32_t>>(std::move(value_segment), buffer_manager);

  buffer_managed_segment->unload_managed_segment();

  auto new_value_segment = buffer_managed_segment->pin_managed_segment();
}

}  // namespace hyrise