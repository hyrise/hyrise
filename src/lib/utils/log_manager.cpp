#include "log_manager.hpp"

#include "constant_mappings.hpp"

namespace opossum {

LogManager::LogManager(SettingsManager& settings_manager) : _log_level(DEFAULT_LOG_LEVEL) {
  if (settings_manager.has_setting(SETTING_NAME)) {
    _log_level_setting = std::dynamic_pointer_cast<LogLevelSetting>(settings_manager.get_setting(SETTING_NAME));
  } else {
    _log_level_setting = std::make_shared<LogLevelSetting>(SETTING_NAME);
    _log_level_setting->register_at(settings_manager);
  }
}

void LogManager::add_message(const std::string& reporter, const std::string& message, const LogLevel log_level) {
  if (log_level < _log_level) return;

  const auto now =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
  const LogEntry log_entry{now, log_level, reporter, message};
  _log_entries.emplace_back(log_entry);
}

const tbb::concurrent_vector<LogEntry>& LogManager::log_entries() const { return _log_entries; }

}  // namespace opossum
