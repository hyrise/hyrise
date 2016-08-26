#include "get_table.hpp"

#include <memory>
#include <string>

#include "storage/storage_manager.hpp"

namespace opossum {

GetTable::GetTable(const std::string &name) : _name(name) {}

const std::string GetTable::get_name() const { return "GetTable"; }

uint8_t GetTable::get_num_in_tables() const { return 0; }

uint8_t GetTable::get_num_out_tables() const { return 1; }

void GetTable::execute() {
  // no expensive execution to be done here
}

std::shared_ptr<Table> GetTable::get_output() const { return StorageManager::get().get_table(_name); }
}  // namespace opossum
