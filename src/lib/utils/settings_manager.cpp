#include "settings_manager.hpp"

namespace hyrise {

bool SettingsManager::has_setting(const std::string& name) const {
  auto lock = std::lock_guard<std::mutex>{_mutex};
  return _settings.contains(name);
}

void SettingsManager::_add(std::shared_ptr<AbstractSetting> setting) {
  auto lock = std::lock_guard<std::mutex>{_mutex};
  Assert(!_settings.contains(setting->name), "A setting with that name already exists.");
  _settings[setting->name] = std::move(setting);
}

void SettingsManager::_remove(const std::string& name) {
  auto lock = std::lock_guard<std::mutex>{_mutex};
  Assert(_settings.contains(name), "A setting with that name does not exist.");
  _settings.erase(name);
}

std::shared_ptr<AbstractSetting> SettingsManager::get_setting(const std::string& name) const {
  auto lock = std::lock_guard<std::mutex>{_mutex};
  Assert(_settings.contains(name), "A setting with that name does not exist.");
  return _settings.at(name);
}

std::vector<std::string> SettingsManager::setting_names() const {
  auto setting_names = std::vector<std::string>{};
  setting_names.reserve(_settings.size());

  {
    auto lock = std::lock_guard<std::mutex>{_mutex};
    for (const auto& [setting_name, _] : _settings) {
      setting_names.emplace_back(setting_name);
    }
  }

  std::sort(setting_names.begin(), setting_names.end());
  return setting_names;
}

}  // namespace hyrise
