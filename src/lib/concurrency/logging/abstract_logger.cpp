#include "abstract_logger.hpp"

#include "logger.hpp"

namespace opossum {

AbstractLogger::AbstractLogger() {
  Logger::create_directories();
}

}  // namespace opossum
