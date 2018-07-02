/*
 *  Logger is a singleton that grants access to the logging system via Logger::getInstance(). 
 *  It instantiates a logging implmenentation and returns it as AbstractLogger. 
 *  The logging system persists database changes to disk that can be used to recover after startup.
 *  A logging implementation therefore provides following functions:
 *    - commit(...)
 *    - value(...)
 *    - invalidate(...)
 *    - flush()
 *    - recover()
 * 
 *  If you want to turn logging off, you need to set NoLogger as the logging implementation.
 * 
 *  When implementing a NewLogger it needs to fullfill following requirements:
 *    -  NewLogger has to be a child of AbstractLogger
 *    -  NewLogger has to implement the methods declared by AbstractLogger
 *    -  The constructor of NewLogger should be private and Logger declared as friend of NewLogger
 *    -  delete copy and copy-assign constructors
 *    -  NewLogger has to be instantiated in Logger::getInstance()
 * 
 *  A new logfile is created on each startup and numbered in ascending order (path = directory + filename + <number>)
 */

#pragma once

#include "abstract_logger.hpp"

#include "types.hpp"

namespace opossum {

class Logger {
 public:

  enum class Implementation {No, Simple, GroupCommit};

  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

  static AbstractLogger& getInstance();

  // Called by logging implementations to get their new filename
  static std::string get_new_log_path();

  static std::vector<std::string> get_all_log_file_paths();

  // Used to set logging implementation on startup or in tests
  static void set_implementation(const Implementation implementation);

  // Used to set the folder of logfiles. Should only be called on startup
  static void set_folder(const std::string folder);

  // Called while setting up a new database or in tests
  static void delete_log_files();

  // create_directories() is called in the constructor of AbstractLogger to ensure their existence.
  static void create_directories();




  // TODO: private
  static u_int32_t _get_latest_log_number();
private:
  static std::string data_path;
  // linter wants these to be char[], but then we loose operator+ of strings
  static std::string log_path;
  static const std::string log_folder;
  static const std::string filename;

  static Implementation _implementation;
  static std::unique_ptr<AbstractLogger> _instance;
};

}  // namespace opossum
