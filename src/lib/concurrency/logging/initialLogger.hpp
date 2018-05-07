#pragma once

#include "types.hpp"

namespace opossum {

class InitialLogger {
 public:  
  InitialLogger(const InitialLogger&) = delete;
  InitialLogger& operator=(const InitialLogger&) = delete;

  static InitialLogger& getInstance();

  void log_commit(const TransactionID transaction_id);

  void flush();

 private:
  InitialLogger();

  int _file_descriptor;
  std::mutex _mutex;
};

}  // namespace opossum
