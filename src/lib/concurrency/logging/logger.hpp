#pragma once

#include "abstract_logger.hpp"

#include "types.hpp"

namespace opossum {

class Logger {
 public:  
  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

  static AbstractLogger& getInstance();
};

}  // namespace opossum