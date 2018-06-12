#include "initial_logger.hpp"

#include "all_type_variant.hpp"

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sstream>

namespace opossum {

void InitialLogger::commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback){
  std::stringstream ss;
  ss << "(t," << transaction_id << ")\n";
  _write_to_logfile(ss);
}

void InitialLogger::value(const TransactionID transaction_id, const std::string table_name, const RowID row_id, const std::vector<AllTypeVariant> values){
  // std::stringstream ss;
  // ss << "(v," << transaction_id << "," << table_name << "," << row_id << "," << values.str() << ")\n";
  // _write_to_logfile(ss);
}

void InitialLogger::invalidate(const TransactionID transaction_id, const std::string table_name, const RowID row_id){
  std::stringstream ss;
  ss << "(i," << transaction_id << "," << table_name << "," << row_id << ")\n";
  _write_to_logfile(ss);
}

InitialLogger::InitialLogger() : AbstractLogger(){};

}  // namespace opossum
