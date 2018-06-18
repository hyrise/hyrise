#pragma once

#include "abstract_logger.hpp"

#include "types.hpp"

namespace opossum {

class Logger {
 public:
  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

  static AbstractLogger& getInstance();
  static void recover();

  static const std::string directory;
  static const std::string filename;
  static const std::string last_log_filename;
};

}  // namespace opossum