#include "logger.hpp"

#include <algorithm>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sstream>
#include <boost/filesystem.hpp>
#include <boost/range.hpp>

#include "abstract_logger.hpp"
#include "group_commit_logger.hpp"
#include "simple_logger.hpp"
#include "no_logger.hpp"

namespace opossum {

// set default Implementation
// Logger::Implementation Logger::_implementation = Implementation::GroupCommit;
Logger::Implementation Logger::_implementation = Implementation::Simple;

AbstractLogger& Logger::getInstance() {
  switch (_implementation) {
    case Implementation::No: { static NoLogger instance; return instance; }
    case Implementation::Simple: { static SimpleLogger instance; return instance; }
    case Implementation::GroupCommit: { static GroupCommitLogger instance; return instance; }
  }
}

void Logger::set_implementation(const Logger::Implementation implementation) {
  _implementation = implementation;
}

void Logger::delete_log_files() {
  boost::filesystem::remove_all(log_path);
  create_directories();
}

void Logger::create_directories() {
  boost::filesystem::create_directory(data_path);
  boost::filesystem::create_directory(log_path);
}

std::string Logger::get_new_log_path() {
  auto log_number = _get_latest_log_number() + 1;
  std::string path = log_path + filename + std::to_string(log_number);
  return path;
}

std::vector<std::string> Logger::get_all_log_file_paths() {
  std::vector<std::string> result;
  for (auto& path : boost::make_iterator_range(boost::filesystem::directory_iterator(log_path), {})) {
    auto pos = path.path().string().rfind(filename);
    if (pos == std::string::npos) {
      continue;
    }
    result.push_back(path.path().string());
  }
  return result;
}

u_int32_t Logger::_get_latest_log_number() {
  u_int32_t max_number{0};

  for (auto& path : boost::make_iterator_range(boost::filesystem::directory_iterator(log_path), {})) {
    auto pos = path.path().string().rfind(filename);
    if (pos == std::string::npos) {
      continue;
    }

    u_int32_t number = std::stoul(path.path().string().substr(pos + filename.length()));
    max_number = std::max(max_number, number);
  }
  return max_number;
}

const std::string Logger::data_path = "./data/";
const std::string Logger::log_path = data_path + "logs/";
const std::string Logger::filename = "hyrise-log";

}  // namespace opossum
