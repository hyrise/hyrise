#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>

#include "optimizer/table_statistics.hpp"

namespace opossum {

// singleton
StorageManager &StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string &name, std::shared_ptr<Table> table) {
  table->set_table_statistics(std::make_shared<TableStatistics>(name, table));
  _tables.insert(std::make_pair(name, std::move(table)));
}

void StorageManager::drop_table(const std::string &name) {
  if (!_tables.erase(name)) {
    throw std::out_of_range("table " + name + " does not exist");
  }
}

std::shared_ptr<Table> StorageManager::get_table(const std::string &name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string &name) const { return _tables.count(name); }

void StorageManager::print(std::ostream &out) const {
  out << "==================" << std::endl;
  out << "===== Tables =====" << std::endl << std::endl;

  auto cnt = 0;
  for (auto const &tab : _tables) {
    out << "==== table >> " << tab.first << " <<";
    out << " (" << tab.second->col_count() << " columns, " << tab.second->row_count() << " rows in "
        << tab.second->chunk_count() << " chunks)";
    out << std::endl << std::endl;
    cnt++;
  }
}

void StorageManager::reset() { get() = StorageManager(); }

}  // namespace opossum
