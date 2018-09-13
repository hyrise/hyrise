/*
 *  Logger is a singleton that grants access to the logging system via Logger::get(). 
 *  It instantiates a logging implementation and returns it as AbstractLogger. 
 *  The logging system persists database changes to disk that can be used to recover after startup.
 *  A logging implementation therefore provides following functions:
 *    - commit(...)
 *    - value(...)
 *    - invalidate(...)
 *    - flush()
 *    - recover()
 * 
 *  Logging is turned on by console and server on startup by calling setup().
 * 
 *  When implementing a NewLogger it needs to fullfill following requirements:
 *    -  NewLogger has to be a child of AbstractLogger
 *    -  NewLogger has to implement the methods declared by AbstractLogger
 *    -  The constructor of NewLogger should be private and Logger declared as friend of NewLogger
 *    -  delete copy and copy-assign constructors
 *    -  NewLogger has to be instantiated in Logger::get()
 * 
 *  A new logfile is created on each startup and numbered in ascending order (path = directory + filename + <number>)
 */

#pragma once

#include "abstract_logger.hpp"
#include "abstract_recoverer.hpp"

#include "types.hpp"

namespace opossum {

class Logger {
 public:
  enum class Implementation { No, Simple, GroupCommit };
  enum class Format { No, Text, Binary };

  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

  static AbstractLogger& get();
  
  // Recovers from log files and returns the number of loaded tables
  static uint32_t recover();

  // Called by logging implementations to get their new filename
  static std::string get_new_log_path();

  // Returns all log file paths in _log_path
  static std::vector<std::string> get_all_log_file_paths();

  // Logging is turned on by console and server on startup by calling setup().
  static void setup(std::string folder, const Implementation implementation, const Format format);

  // Called while setting up a new database in console or in tests
  // Current logging implementation has to be shut down beforehand or not instantiated yet.
  static void delete_log_files();

  // Returns if there is any logging set or turned off by NoLogger
  static bool is_active();

 private:
  friend class LogFileTest;

  // Used by tests to set logging off after test. Should not be used during normal runtime.
  static void reset_to_no_logger();

  // Creates the needed folders where logs are saved
  static void _create_directories();

  // Returns the the highest logfile number found in _log_path
  static u_int32_t _get_latest_log_number();

  // linter wants these to be char[], but then we loose operator+ of strings
  static std::string _data_path;
  static std::string _log_path;
  static const std::string _log_folder;
  static const std::string _filename;

  static Implementation _implementation;
  static std::unique_ptr<AbstractLogger> _logger_instance;
};

}  // namespace opossum
