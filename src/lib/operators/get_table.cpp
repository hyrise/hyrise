#include "get_table.hpp"

#include <memory>
#include <string>
#include <vector>

#include "storage/storage_manager.hpp"

namespace opossum {

GetTable::GetTable(const std::string& name) : _name(name) {}

const std::string GetTable::name() const { return "GetTable"; }

const std::string GetTable::description() const { return std::string("GetTable(") + table_name() + ")"; }

uint8_t GetTable::num_in_tables() const { return 0; }

uint8_t GetTable::num_out_tables() const { return 1; }

const std::string& GetTable::table_name() const { return _name; }

std::shared_ptr<AbstractOperator> GetTable::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<GetTable>(_name);
}

std::shared_ptr<const Table> GetTable::_on_execute() { return StorageManager::get().get_table(_name); }
}  // namespace opossum
