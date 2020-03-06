#include "base_test.hpp"

#include "./mock_setting.hpp"

namespace opossum {

class SettingTest : public BaseTest {
 protected:
  void SetUp() {
    Hyrise::reset();
    mock_setting = std::make_shared<MockSetting>("mock_setting");
  }

  void TearDown() { Hyrise::reset(); }

  std::shared_ptr<AbstractSetting> mock_setting;
};

TEST_F(SettingTest, RegistrationAtSettingsManager) {
  auto& settings_manager = Hyrise::get().settings_manager;
  EXPECT_FALSE(settings_manager.has_setting("mock_setting"));
  mock_setting->register_at_settings_manager();
  EXPECT_TRUE(settings_manager.has_setting("mock_setting"));
  mock_setting->unregister_at_settings_manager();
  EXPECT_FALSE(settings_manager.has_setting("mock_setting"));
}

}  // namespace opossum
