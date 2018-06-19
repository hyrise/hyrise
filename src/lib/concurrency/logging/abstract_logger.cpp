#include "abstract_logger.hpp"

#include <fstream>

#include "logger.hpp"

namespace opossum {

u_int32_t AbstractLogger::_get_new_log_number() {
  std::ifstream last_log_number_file(Logger::directory + Logger::last_log_filename, std::ios::in);
  u_int32_t log_number;
  last_log_number_file >> log_number;
  last_log_number_file.close();
  ++log_number;
  return log_number;
}

void AbstractLogger::_set_last_log_number(u_int32_t log_number) {
  std::ofstream last_log_number_file(Logger::directory + Logger::last_log_filename, std::ios::out | std::ofstream::trunc);
  last_log_number_file << std::to_string(log_number);
  last_log_number_file.close();
}

}  // namespace opossum
