#include "logger.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sstream>

#include "abstract_logger.hpp"
#include "group_commit_logger.hpp"
#include "simple_logger.hpp"
#include "no_logger.hpp"

namespace opossum {

AbstractLogger& Logger::getInstance() {
  static SimpleLogger instance;
  // static GroupCommitLogger instance;
  // static NoLogger instance;
  return instance;
}

u_int32_t Logger::_get_latest_log_number() {
  std::ifstream latest_log_number_file(Logger::directory + Logger::last_log_filename, std::ios::in);
  u_int32_t log_number;
  latest_log_number_file >> log_number;
  latest_log_number_file.close();
  return log_number;
}

void Logger::_set_latest_log_number(u_int32_t log_number) {
  std::ofstream latest_log_number_file(Logger::directory + Logger::last_log_filename, std::ios::out | std::ofstream::trunc);
  latest_log_number_file << std::to_string(log_number);
  latest_log_number_file.close();
}

const std::string Logger::directory = "/Users/Dimitri/transaction_logs/";
const std::string Logger::filename = "hyrise-log";
const std::string Logger::last_log_filename = "last_log_number.txt";

}  // namespace opossum
