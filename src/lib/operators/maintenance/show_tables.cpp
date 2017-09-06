#include "show_tables.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "storage/storage_manager.hpp"

namespace opossum {

ShowTables::ShowTables(std::ostream& out) : _out(out) {}

uint8_t ShowTables::num_in_tables() const { return 0; }

uint8_t ShowTables::num_out_tables() const { return 0; }

const std::string ShowTables::name() const { return "ShowTables"; }

std::shared_ptr<AbstractOperator> ShowTables::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<ShowTables>(_out);
}

std::shared_ptr<const Table> ShowTables::on_execute() {
  const auto table_names = StorageManager::get().table_names();

  for (const auto& table_name : table_names) {
    _out << " " << table_name << std::endl;
  }

  _out << std::endl << table_names.size() << " tables." << std::endl;

  return std::make_shared<Table>();
}

}  // namespace opossum
