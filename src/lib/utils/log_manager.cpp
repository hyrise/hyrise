#include "log_manager.hpp"

namespace opossum {

void LogManager::add_message(const std::string& reporter, const std::string& message, const LogLevel log_level) {
  const auto now = std::chrono::system_clock::now();
  const LogEntry log_entry{now, log_level, reporter, message};
  _log_entries.push_back(log_entry);
}

const tbb::concurrent_vector<LogEntry>& LogManager::log_entries() const { return _log_entries; }

}  // namespace opossum
