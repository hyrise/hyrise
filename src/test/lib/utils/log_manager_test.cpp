#include "base_test.hpp"

#include "utils/log_manager.hpp"

namespace opossum {

class LogManagerTest : public BaseTest {
 protected:
  void SetUp() {
    settings_manager = SettingsManager{};
    log_manager = LogManager{settings_manager};
  }

  SettingsManager settings_manager = SettingsManager{};
  LogManager log_manager = LogManager{settings_manager};
};

TEST_F(LogManagerTest, InsertMessage) {
  log_manager.add_message("foo", "bar", LogLevel::Error);
  EXPECT_EQ(log_manager.log_entries().size(), 1);
  const auto& entry = log_manager.log_entries()[0];
  EXPECT_EQ(entry.reporter, "foo");
  EXPECT_EQ(entry.message, "bar");
  EXPECT_EQ(entry.log_level, LogLevel::Error);
}

TEST_F(LogManagerTest, DiscardLowerMessage) {
  log_manager.add_message("foo", "bar", LogLevel::Debug);
  EXPECT_EQ(log_manager.log_entries().size(), 0);
}

TEST_F(LogManagerTest, RegisterSetting) {
  EXPECT_TRUE(settings_manager.has_setting(LogManager::LOG_LEVEL_SETTING_NAME));
}

}  // namespace opossum
