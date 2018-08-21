#include "abstract_logger.hpp"

#include "logger.hpp"

namespace opossum {

AbstractLogger::AbstractLogger(std::unique_ptr<AbstractFormatter> formatter) : _formatter(std::move(formatter)) { 
  _open_logfile(); 
}

void AbstractLogger::_open_logfile() {
  DebugAssert(!_log_file.is_open(), "Logger: Log file not closed before opening another one.");
  {
  std::scoped_lock file_lock(_file_mutex);

  auto path = Logger::get_new_log_path();
  _log_file.open(path, std::ios::out);
  }
}

}  // namespace opossum
