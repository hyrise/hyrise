#pragma once

#include <chrono>
#include <functional>
#include <tbb/concurrent_vector.h>

#include "types.hpp"
#include "utils/settings/abstract_setting.hpp"

namespace opossum {

struct LogEntry {
  std::chrono::milliseconds timestamp;
  std::string sender;
  std::string message;
};

class LogManager : public Noncopyable {
 public:
  void add_message(const std::string& sender, const std::string& message);

  const tbb::concurrent_vector<LogEntry>& log_entries() const;

 private:
  tbb::concurrent_vector<LogEntry> _log_entries;
};

}  // namespace opossum
