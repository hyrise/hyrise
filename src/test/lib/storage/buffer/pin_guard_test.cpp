#include "base_test.hpp"
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/pin_guard.hpp"

namespace hyrise {

class PinGuardTest : public BaseTest {
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

TEST_F(PinGuardTest, TestExclusivePinGuard) {
    {
        auto pin_guard = Excl
    }
}

TEST_F(PinGuardTest, TestSharedPinGuard) {}

}  // namespace hyrise