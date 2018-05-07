#include "initialLogger.hpp"

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sstream>

namespace opossum {

InitialLogger& InitialLogger::getInstance() {
  static InitialLogger instance;
  return instance;
}

void InitialLogger::log_commit(const TransactionID transaction_id){
  std::stringstream ss;
  ss << "(t," << transaction_id << ")";

  _mutex.lock();
  write(_file_descriptor, (void*)ss.str().c_str(), ss.str().length());
  _mutex.unlock();
}

void InitialLogger::flush() {
  fsync(_file_descriptor);
}

InitialLogger::InitialLogger(){
  std::string directory = "/Users/Dimitri/";
  std::string filename = directory + "hyrise-log.txt";

  // TODO: what if directory does not exists?

  // other rights?
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  int oflags = O_WRONLY | O_CREAT;

  _file_descriptor = open(filename.c_str(), oflags, mode);
  DebugAssert(_file_descriptor != -1, "Logfile could not be opened / created: " + filename);
}

}  // namespace opossum
