#include "initialLogger.hpp"

#include <sstream>

namespace opossum {

InitialLogger& InitialLogger::getInstance() {
  static InitialLogger instance;
  return instance;
}

void InitialLogger::log_commit(const TransactionID transaction_id){
  std::stringstream ss;
  ss << "(t," << transaction_id << ")";
  std::cout << ss.str() << std::endl;
}

InitialLogger::InitialLogger(){
  
}

}  // namespace opossum
