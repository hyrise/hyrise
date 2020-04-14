#include "base_test.hpp"

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "utils/settings/log_level_setting.hpp"
#include "utils/settings_manager.hpp"

namespace opossum {

class LogLevelSettingTest : public BaseTest {
 protected:
  void SetUp() {
    settings_manager = SettingsManager{};
    setting = std::make_shared<LogLevelSetting>(LogManager::LOG_LEVEL_SETTING_NAME);
  }

  LogLevel get_log_level() { return Hyrise::get().log_manager._log_level; }

  SettingsManager settings_manager = SettingsManager{};
  std::shared_ptr<LogLevelSetting> setting;
};

TEST_F(LogLevelSettingTest, RegistersSelf) {
  setting->register_at(settings_manager);
  EXPECT_TRUE(settings_manager.has_setting(LogManager::LOG_LEVEL_SETTING_NAME));
}

TEST_F(LogLevelSettingTest, ShowsCorrectLevel) {
  const auto log_level = log_level_to_string.left.at(get_log_level());
  EXPECT_EQ(setting->get(), log_level);
}

TEST_F(LogLevelSettingTest, ChangesLevel) {
  setting->set("Error");
  EXPECT_EQ(get_log_level(), LogLevel::Error);
}

TEST_F(LogLevelSettingTest, FailsWithUnknownLevel) { EXPECT_THROW(setting->set("unknown_log_level"), std::exception); }

}  // namespace opossum
