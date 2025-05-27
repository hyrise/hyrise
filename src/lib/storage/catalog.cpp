#include "catalog.hpp"

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

Catalog& Catalog::operator=(Catalog&& other) noexcept {
  _table_ids = std::move(other._table_ids);
  _table_names = std::move(other._table_names);
  _next_table_id = other._next_table_id.load();
  return *this;
}

TableID Catalog::register_table(const std::string& name) {
  const auto iter = _table_ids.find(name);
  Assert(iter == _table_ids.end() || iter->second == INVALID_TABLE_ID,
         "Cannot add table " + name + " - a table with the same name already exists.");
  const auto table_id = _next_table_id++;
  _table_names.grow_to_at_least(table_id);
  _table_names[table_id] = name;
  _table_ids[name] = table_id;
  return static_cast<TableID>(table_id);
}

void Catalog::deregister_table(TableID table_id) {
  Assert(table_id < _table_names.size(), "TableID " + std::to_string(table_id) + " out of range.");
  deregister_table(_table_names[table_id]);
}

void Catalog::deregister_table(const std::string& name) {
  const auto iter = _table_ids.find(name);
  Assert(iter != _table_ids.end() && iter->second != INVALID_TABLE_ID,
         "Error deleting table. No such table named '" + name + "'");

  // The `concurrent_unordered_map` does not support concurrency-safe erasure. Thus, we simply reset the table ID.
  _table_ids[name] = INVALID_TABLE_ID;
}

TableID Catalog::table_id(const std::string& name) const {
  const auto iter = _table_ids.find(name);
  return iter == _table_ids.end() ? INVALID_TABLE_ID : iter->second;
}

const std::string& Catalog::table_name(const TableID table_id) const {
  Assert(table_id < _table_names.size(), "TableID " + std::to_string(table_id) + " out of range.");
  return _table_names[table_id];
}

std::vector<std::string_view> Catalog::table_names() const {
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

std::unordered_map<std::string_view, TableID> Catalog::table_ids() const {
  auto result = std::unordered_map<std::string_view, TableID>{};

  for (const auto& [table_name, table_id] : _table_ids) {
    if (table_id != INVALID_TABLE_ID) {
      result[table_name] = table_id;
    }
  }
  return result;
}

}  // namespace hyrise
