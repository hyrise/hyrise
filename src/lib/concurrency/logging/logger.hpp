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

  // linter wants these to be char[], but then I loose operator+ of strings
  static const std::string directory;
  static const std::string filename;
  static const std::string last_log_filename;
};

}  // namespace opossum
