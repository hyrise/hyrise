#include "abstract_recoverer.hpp"

#include "concurrency/transaction_manager.hpp"
#include "logged_item.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace opossum {

void AbstractRecoverer::_redo_transaction(
    std::map<TransactionID, std::vector<std::unique_ptr<LoggedItem>>>& transactions, TransactionID transaction_id) {
  auto transaction = transactions.find(transaction_id);
  if (transaction == transactions.end()) return;

  for (auto& item : transaction->second) {
    item->redo();
  }

  transactions.erase(transaction);
}

void AbstractRecoverer::_redo_load_table(const std::string& path, const std::string& table_name) {
  auto table = load_table(path, Chunk::MAX_SIZE);
  StorageManager::get().add_table(table_name, table);
  ++_number_of_loaded_tables;
}

}  // namespace opossum
