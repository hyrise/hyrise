#include "storage_manager.hpp"

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
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
#include "storage/catalog.hpp"
#include "storage/lqp_view.hpp"
#include "storage/prepared_plan.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/meta_table_manager.hpp"

namespace hyrise {

void StorageManager::_add_table(const ObjectID table_id, std::shared_ptr<Table> table) {
  const auto needs_growth = table_id >= _tables.size();
  Assert(needs_growth || !_tables[table_id],
         "Cannot add table " + std::to_string(table_id) + " - a table with the same ID already exists.");
  if (needs_growth) {
    _tables.grow_to_at_least(table_id);
  }

  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    // We currently assume that all tables stored in the StorageManager are mutable and, as such, have MVCC data. This
    // way, we do not need to check query plans if they try to update immutable tables. However, this is not a hard
    // limitation and might be changed into more fine-grained assertions if the need arises.
    Assert(table->get_chunk(chunk_id)->has_mvcc_data(), "Table must have MVCC data.");
  }

  // Create table statistics and chunk pruning statistics for added table.
  if (!table->table_statistics()) {
    table->set_table_statistics(TableStatistics::from_table(*table));
  }
  generate_chunk_pruning_statistics(table);

  _tables[table_id] = std::move(table);
}

void StorageManager::_drop_table(const ObjectID table_id) {
  Assert(table_id < _tables.size() && _tables[table_id],
         "Error deleting table. No such table with ID '" + std::to_string(table_id) + "'.");
  _tables[table_id] = nullptr;
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  const auto table_id = Hyrise::get().catalog.table_id(name);
  Assert(table_id != INVALID_OBJECT_ID, "No such table named '" + name + "'.");

  return get_table(table_id);
}

std::shared_ptr<Table> StorageManager::get_table(const ObjectID table_id) const {
  Assert(table_id < _tables.size() && _tables[table_id],
         "No such table with ID '" + std::to_string(table_id) + "'. Was it dropped?");
  return _tables[table_id];
}

bool StorageManager::has_table(const std::string& name) const {
  return Hyrise::get().catalog.table_id(name) != INVALID_OBJECT_ID;
}

bool StorageManager::has_table(const ObjectID table_id) const {
  return _tables[table_id] != nullptr;
}

void StorageManager::_add_view(const ObjectID view_id, const std::shared_ptr<LQPView>& view) {
  const auto needs_growth = view_id >= _views.size();
  Assert(needs_growth || !_views[view_id],
         "Cannot add view " + std::to_string(view_id) + " - a view with the same ID already exists.");
  if (needs_growth) {
    _views.grow_to_at_least(view_id);
  }

  _views[view_id] = view;
}

void StorageManager::_drop_view(const ObjectID view_id) {
  Assert(view_id < _views.size() && _views[view_id],
         "Error deleting view. No such view with ID '" + std::to_string(view_id) + "'");
  _views[view_id] = nullptr;
}

std::shared_ptr<LQPView> StorageManager::get_view(const std::string& name) const {
  const auto view_id = Hyrise::get().catalog.view_id(name);
  Assert(view_id != INVALID_OBJECT_ID, "No such view named '" + name + "'.");

  return get_view(view_id);
}

std::shared_ptr<LQPView> StorageManager::get_view(const ObjectID view_id) const {
  Assert(view_id < _views.size() && _views[view_id],
         "No such view with ID '" + std::to_string(view_id) + "'. Was it dropped?");
  return _views[view_id]->deep_copy();
}

bool StorageManager::has_view(const std::string& name) const {
  return Hyrise::get().catalog.view_id(name) != INVALID_OBJECT_ID;
}

bool StorageManager::has_view(const ObjectID view_id) const {
  return view_id < _views.size() && _views[view_id] != nullptr;
}

void StorageManager::_add_prepared_plan(const ObjectID plan_id, const std::shared_ptr<PreparedPlan>& prepared_plan) {
  const auto needs_growth = plan_id >= _prepared_plans.size();
  Assert(needs_growth || !_prepared_plans[plan_id],
         "Cannot add prepared plan " + std::to_string(plan_id) + " - a view with the same ID already exists");
  if (needs_growth) {
    _prepared_plans.grow_to_at_least(plan_id);
  }

  _prepared_plans[plan_id] = prepared_plan;
}

void StorageManager::_drop_prepared_plan(const ObjectID plan_id) {
  Assert(plan_id < _prepared_plans.size() && _prepared_plans[plan_id],
         "Error deleting prepared plan. No such prepared plan with ID '" + std::to_string(plan_id) + "'");
  _prepared_plans[plan_id] = nullptr;
}

std::shared_ptr<PreparedPlan> StorageManager::get_prepared_plan(const std::string& name) const {
  const auto plan_id = Hyrise::get().catalog.prepared_plan_id(name);
  Assert(plan_id != INVALID_OBJECT_ID, "No such prepared plan named '" + name + "'.");

  return get_prepared_plan(plan_id);
}

std::shared_ptr<PreparedPlan> StorageManager::get_prepared_plan(const ObjectID plan_id) const {
  Assert(plan_id < _prepared_plans.size() && _prepared_plans[plan_id],
         "No such prepared plan with ID '" + std::to_string(plan_id) + "'. Was it dropped?");
  return _prepared_plans[plan_id];
}

bool StorageManager::has_prepared_plan(const std::string& name) const {
  return Hyrise::get().catalog.prepared_plan_id(name) != INVALID_OBJECT_ID;
}

bool StorageManager::has_prepared_plan(const ObjectID plan_id) const {
  return plan_id < _prepared_plans.size() && _prepared_plans[plan_id] != nullptr;
}

}  // namespace hyrise
