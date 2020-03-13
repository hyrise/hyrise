#pragma once

#include <tbb/concurrent_vector.h>
#include <chrono>
#include <functional>

#include "types.hpp"
#include "utils/settings/abstract_setting.hpp"

namespace opossum {

struct LogEntry {
  std::chrono::milliseconds timestamp;
  std::string reporter;
  std::string message;
};

class LogManager : public Noncopyable {
 public:
  void add_message(const std::string& reporter, const std::string& message);

  const tbb::concurrent_vector<LogEntry>& log_entries() const;

 private:
  tbb::concurrent_vector<LogEntry> _log_entries;
};

}  // namespace opossum
