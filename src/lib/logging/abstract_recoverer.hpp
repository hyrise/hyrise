#pragma once

#include "all_type_variant.hpp"
#include "logged_item.hpp"
#include "types.hpp"

namespace opossum {

/*  
 *  AbstractRecoverer is the abstract class for all recovery implementations. 
 *  It serves functionality that is used by all recoverers, namely 
 *   -  recovery from a file
 *   -  replay a transaction
 */

class AbstractRecoverer {
 public:
  AbstractRecoverer(const AbstractRecoverer&) = delete;
  AbstractRecoverer& operator=(const AbstractRecoverer&) = delete;

  // Recovers db from logfiles and returns the number of loaded tables
  virtual uint32_t recover() = 0;

 protected:
  AbstractRecoverer() {}

  void _redo_transaction(std::map<TransactionID, std::vector<std::unique_ptr<LoggedItem>>>& transactions,
                         TransactionID transaction_id);

  void _redo_load_table(const std::string& path, const std::string& table_name);

  uint32_t _number_of_loaded_tables;
};

}  // namespace opossum
