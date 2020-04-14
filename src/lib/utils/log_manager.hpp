#pragma once

#include <tbb/concurrent_vector.h>
#include <chrono>
#include <functional>

#include "types.hpp"
#include "utils/settings/log_level_setting.hpp"
#include "utils/settings_manager.hpp"

namespace opossum {

struct LogEntry {
  std::chrono::milliseconds timestamp;
  LogLevel log_level;
  std::string reporter;
  std::string message;
};

class LogManager : public Noncopyable {
 public:
  constexpr static LogLevel DEFAULT_LOG_LEVEL = LogLevel::Info;
  constexpr static char SETTING_NAME[] = "LogManager.log_level";

  void add_message(const std::string& reporter, const std::string& message, const LogLevel log_level = LogLevel::Debug);

  const tbb::concurrent_vector<LogEntry>& log_entries() const;

 protected:
  friend class Hyrise;
  friend class LogLevelSetting;

  explicit LogManager(SettingsManager& settings_manager);

  LogLevel _log_level;

 private:
  std::shared_ptr<LogLevelSetting> _log_level_setting;
  tbb::concurrent_vector<LogEntry> _log_entries;
};

}  // namespace opossum
