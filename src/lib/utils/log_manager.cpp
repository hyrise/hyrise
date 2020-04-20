#include "log_manager.hpp"

#include "constant_mappings.hpp"
#include "utils/settings/log_level_setting.hpp"

namespace opossum {

LogManager::LogManager(SettingsManager& settings_manager, LogLevel log_level) : _log_level(log_level) {
  if (!settings_manager.has_setting(LOG_LEVEL_SETTING_NAME)) {
    auto log_level_setting = std::make_shared<LogLevelSetting>(LOG_LEVEL_SETTING_NAME);
    log_level_setting->register_at(settings_manager);
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
