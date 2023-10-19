#include "base_test.hpp"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

class BufferManagerTest : public BaseTest {
 public:
  void SetUp() override {
    std::filesystem::create_directory(db_path);
    buffer_manager = std::make_unique<BufferManager>(1 << 20, db_path);
  }

  void TearDown() override {
    buffer_manager = nullptr;
  }

  const std::filesystem::path db_path = test_data_path + "buffer_manager_data";
  std::unique_ptr<BufferManager> buffer_manager;
};

TEST_F(BufferManagerTest, TestPinning) {}

}  // namespace hyrise