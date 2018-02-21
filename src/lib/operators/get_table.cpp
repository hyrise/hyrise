#include "get_table.hpp"

#include <memory>
#include <string>
#include <vector>

#include "storage/storage_manager.hpp"

namespace opossum {

GetTable::GetTable(const std::string& name) : _name(name) {}

const std::string GetTable::name() const { return "GetTable"; }

const std::string GetTable::description(DescriptionMode description_mode) const {
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  return name() + separator + "(" + table_name() + ")";
}

const std::string& GetTable::table_name() const { return _name; }

std::shared_ptr<AbstractOperator> GetTable::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<GetTable>(_name);
}

std::shared_ptr<const Table> GetTable::_on_execute() { return StorageManager::get().get_table(_name); }
}  // namespace opossum
