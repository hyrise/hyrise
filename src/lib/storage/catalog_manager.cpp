#include "catalog_manager.hpp"

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "import_export/file_type.hpp"
#include "operators/export.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/lqp_view.hpp"
#include "storage/prepared_plan.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/meta_table_manager.hpp"

namespace hyrise {

CatalogManager& CatalogManager::operator=(CatalogManager&& other) noexcept {
  _table_ids = std::move(other._table_ids);
  _next_table_id = other._next_table_id.load();
  return *this;
}

TableID CatalogManager::register_table(const std::string& name) {
  const auto iter = _table_ids.find(name);
  Assert(iter == _table_ids.end() || iter->second == INVALID_TABLE_ID,
         "Cannot add table " + name + " - a table with the same name already exists");
  const auto table_id = _next_table_id++;
  _table_ids[name] = table_id;
  return static_cast<TableID>(table_id);
}

void CatalogManager::deregister_table(const std::string& name) {
  const auto iter = _table_ids.find(name);
  Assert(iter != _table_ids.end() && iter->second != INVALID_TABLE_ID,
         "Error deleting table. No such table named '" + name + "'");

  // The concurrent_unordered_map does not support concurrency-safe erasure. Thus, we simply reset the table ID.
  _table_ids[name] = INVALID_TABLE_ID;
}

TableID CatalogManager::get_table_id(const std::string& name) {
  const auto iter = _table_ids.find(name);
  if (iter == _table_ids.end()) {
    return INVALID_TABLE_ID;
  }
  return iter->second;
}

std::vector<std::string_view> CatalogManager::table_names() const {
  auto table_names = std::vector<std::string_view>{};
  table_names.reserve(_table_ids.size());

  for (const auto& [table_name, table_id] : _table_ids) {
    if (table_id == INVALID_TABLE_ID) {
      continue;
    }

    table_names.emplace_back(table_name);
  }

  std::sort(table_names.begin(), table_names.end());
  return table_names;
}

std::unordered_map<std::string_view, TableID> CatalogManager::table_ids() const {
  auto result = std::unordered_map<std::string_view, TableID>{};

  for (const auto& [table_name, table_id] : _table_ids) {
    if (table_id != INVALID_TABLE_ID) {
      result[table_name] = table_id;
    }
  }
  return result;
}

}  // namespace hyrise
