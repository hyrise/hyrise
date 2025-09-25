#include "log_manager.hpp"

#include <chrono>
#include <string>

#include <oneapi/tbb/concurrent_vector.h>  // NOLINT(build/include_order): cpplint identifies TBB as C system headers.

namespace hyrise {

void LogManager::add_message(const std::string& reporter, const std::string& message, const LogLevel log_level) {
  const auto now = std::chrono::system_clock::now();
  _log_entries.emplace_back(now, log_level, reporter, message);
}

const tbb::concurrent_vector<LogEntry>& LogManager::log_entries() const {
  return _log_entries;
}

}  // namespace hyrise
