#include "settings_manager.hpp"

namespace opossum {

bool SettingsManager::has_setting(const std::string& name) const { return _settings.count(name); }

void SettingsManager::add(const std::shared_ptr<AbstractSetting>& setting) {
  Assert(!_settings.count(setting->name()), "A setting with that name already exists.");
  _settings[setting->name()] = setting;
}

const std::shared_ptr<AbstractSetting> SettingsManager::get(const std::string& name) const {
  Assert(_settings.count(name), "A setting with that name does not exist.");
  return _settings.at(name);
}

const std::vector<std::shared_ptr<AbstractSetting>> SettingsManager::all_settings() const {
  std::vector<std::shared_ptr<AbstractSetting>> settings_list;
  settings_list.reserve(_settings.size());

  for (const auto& [_, setting] : _settings) {
    settings_list.emplace_back(setting);
  }

  std::sort(settings_list.begin(), settings_list.end());

  return settings_list;
}

}  // namespace opossum
