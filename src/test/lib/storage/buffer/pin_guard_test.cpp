#include "base_test.hpp"
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/pin_guard.hpp"

namespace hyrise {

class PinGuardTest : public BaseTest {
 public:
  struct PinnableTestType {};

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

template <>
struct Pinnnable<PinnableTestType> {
  static PageIDContainer get_page_ids(const T& pinnable) {
    return PageIDContainer{1337};
  }
};

// TODO Test Movable

TEST_F(PinGuardTest, ExclusivePinGuard) {
  {
    auto test_type = PinnableTestType{};
    auto pin_guard = ExclusivePinGuard{test_type, *buffer_manager};
  }

  {
    // TODO: leak, test mark dirty
    auto test_type = PolymorphicAllocator<PinnableTestType>{buffer_manager->get_memory_resource()}.allocate(1);
    auto pin_guard = ExclusivePinGuard{test_type, *buffer_manager};
  }
  {
    // Test movable
    auto test_type = PinnableTestType{};
    auto pin_guard = ExclusivePinGuard{test_type, *buffer_manager};
    auto pin_guard2 = std::move(pin_guard);
  }
}

TEST_F(PinGuardTest, SharedPinGuard) {}

}  // namespace hyrise