#pragma once

#include "types.hpp"

namespace opossum {

class InitialLogger {
 public:  
  InitialLogger(const InitialLogger&) = delete;
  InitialLogger& operator=(const InitialLogger&) = delete;

  static InitialLogger& getInstance();

  void log_commit(const TransactionID transaction_id);

 private:
  InitialLogger();
};

}  // namespace opossum
