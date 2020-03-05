#include "../base_test.hpp"

#include "./mock_setting.hpp"
#include "hyrise.hpp"

namespace opossum {

class SettingsManagerTest : public BaseTest {
 protected:
  void SetUp() {
    Hyrise::reset();
    mock_setting = std::make_shared<MockSetting>("mock_setting");
    another_mock_setting = std::make_shared<MockSetting>("mock_setting");
  }

  void TearDown() { Hyrise::reset(); }

  void add_setting(std::shared_ptr<AbstractSetting> setting) { Hyrise::get().settings_manager._add(setting); }

  void remove_setting(const std::string& name) { Hyrise::get().settings_manager._remove(name); }

  std::shared_ptr<AbstractSetting> mock_setting;
  std::shared_ptr<AbstractSetting> another_mock_setting;
};

TEST_F(SettingsManagerTest, LoadUnloadSetting) {
  auto& settings_manager = Hyrise::get().settings_manager;

  EXPECT_FALSE(settings_manager.has_setting("mock_setting"));

  add_setting(mock_setting);
  EXPECT_TRUE(settings_manager.has_setting("mock_setting"));

  auto stored_setting = settings_manager.get_setting("mock_setting");

  EXPECT_EQ(stored_setting->name, mock_setting->name);
  EXPECT_EQ(stored_setting->get(), mock_setting->get());
  EXPECT_EQ(stored_setting->description(), mock_setting->description());

  remove_setting("mock_setting");

  EXPECT_FALSE(settings_manager.has_setting("mock_setting"));
}

TEST_F(SettingsManagerTest, LoadingSameName) {
  auto& settings_manager = Hyrise::get().settings_manager;

  EXPECT_FALSE(settings_manager.has_setting("mock_setting"));

  add_setting(mock_setting);

  EXPECT_THROW(add_setting(another_mock_setting), std::exception);
}

TEST_F(SettingsManagerTest, UnloadNotLoadedSetting) {
  auto& settings_manager = Hyrise::get().settings_manager;

  EXPECT_FALSE(settings_manager.has_setting("not_existing_setting"));

  EXPECT_THROW(remove_setting("not_existing_setting"), std::exception);
}

TEST_F(SettingsManagerTest, GetNotLoadedSetting) {
  auto& settings_manager = Hyrise::get().settings_manager;

  EXPECT_FALSE(settings_manager.has_setting("not_existing_setting"));

  EXPECT_THROW(settings_manager.get_setting("not_existing_setting"), std::exception);
}

}  // namespace opossum
