#include "logger.hpp"
#include "abstract_logger.hpp"
#include "initial_logger.hpp"
#include "group_commit_logger.hpp"

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sstream>

namespace opossum {

AbstractLogger& Logger::getInstance() {
  // static InitialLogger instance;
  static GroupCommitLogger instance;
  return instance;
}

void Logger::recover(){
  getInstance().recover();
}

const std::string Logger::directory = "/Users/Dimitri/transaction_logs/";
const std::string Logger::filename = "hyrise-log";
const std::string Logger::last_log_filename = "last_log_number.txt";

}  // namespace opossum
