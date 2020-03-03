#include "settings_manager.hpp"

#include "utils/settings/mock_setting.hpp"

namespace opossum {

bool SettingsManager::has_setting(const std::string& name) const { return _settings.count(name); }

void SettingsManager::add(const std::string& name, std::shared_ptr<AbstractSetting> setting) {
  Assert(!_settings.count(name), "A setting with that name already exists.");
  _settings[name] = std::move(setting);
}

void SettingsManager::remove(const std::string name) {
  Assert(_settings.count(name), "A setting with that name does not exist.");
  _settings.erase(name);
}

std::shared_ptr<AbstractSetting> SettingsManager::get_setting(const std::string name) const {
  Assert(_settings.count(name), "A setting with that name does not exist.");
  return _settings.at(name);
}

std::vector<std::string> SettingsManager::all_settings() const {
  std::vector<std::string> settings_list;
  settings_list.reserve(_settings.size());

  for (const auto& [setting_name, _] : _settings) {
    settings_list.emplace_back(setting_name);
  }
  std::sort(settings_list.begin(), settings_list.end());

  return settings_list;
}

}  // namespace opossum
