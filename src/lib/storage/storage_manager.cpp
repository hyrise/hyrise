#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>

namespace opossum {

StorageManager &StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string &name, std::shared_ptr<Table> tp) {
  _tables.insert(std::make_pair(name, std::move(tp)));
}

std::shared_ptr<Table> StorageManager::get_table(const std::string &name) const { return _tables.at(name); }

void StorageManager::print(std::ostream &out) const {
  out << "==================" << std::endl;
  out << "===== Tables =====" << std::endl << std::endl;

  auto cnt = 0;
  for (auto const &tab : _tables) {
    out << "==== table >> " << tab.first << " <<";
    out << " (" << tab.second->col_count() << " columns, " << tab.second->row_count() << " rows in "
        << tab.second->chunk_count() << " chunks)";
    out << std::endl << std::endl;
    tab.second->print();
    cnt++;
  }
}
}  // namespace opossum
