#include "get_table.hpp"

#include <memory>
#include <string>

#include "storage/storage_manager.hpp"

namespace opossum {

GetTable::GetTable(const std::string &name) : _name(name) {}

const std::string GetTable::name() const { return "GetTable"; }

uint8_t GetTable::num_in_tables() const { return 0; }

uint8_t GetTable::num_out_tables() const { return 1; }

std::shared_ptr<const Table> GetTable::on_execute() { return StorageManager::get().get_table(_name); }
}  // namespace opossum
