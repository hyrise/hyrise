#include "storage_manager.hpp"

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <oneapi/tbb/concurrent_unordered_map.h>  // NOLINT(build/include_order): Identified as C system headers.

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

void StorageManager::_add_table(const ObjectID table_id, const std::shared_ptr<Table>& table) {
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

  const auto inserted = _tables.emplace(table_id, table);
  Assert(inserted, "Cannot add table " + std::to_string(table_id) + " - a table with the same ID already exists.");
}

void StorageManager::_drop_table(const ObjectID table_id) {
  const auto erased = _tables.erase(table_id);
  Assert(erased, "Error deleting table. No such table with ID " + std::to_string(table_id) + ".");
}

std::shared_ptr<Table> StorageManager::get_table(const ObjectID table_id) const {
  auto accessor = decltype(_tables)::const_accessor{};
  _tables.find(accessor, table_id);
  Assert(!accessor.empty(), "No such table with ID " + std::to_string(table_id) + ".");
  return accessor->second;
}

bool StorageManager::has_table(const ObjectID table_id) const {
  return _tables.count(table_id) != 0;
}

void StorageManager::_add_view(const ObjectID view_id, const std::shared_ptr<LQPView>& view) {
  const auto inserted = _views.emplace(view_id, view);
  Assert(inserted, "Cannot add view " + std::to_string(view_id) + " - a view with the same ID already exists.");
}

void StorageManager::_drop_view(const ObjectID view_id) {
  const auto erased = _views.erase(view_id);
  Assert(erased, "Error deleting view. No such view with ID " + std::to_string(view_id) + ".");
}

std::shared_ptr<LQPView> StorageManager::get_view(const ObjectID view_id) const {
  auto accessor = decltype(_views)::const_accessor{};
  _views.find(accessor, view_id);
  Assert(!accessor.empty(), "No such view with ID " + std::to_string(view_id) + ".");
  return accessor->second->deep_copy();
}

bool StorageManager::has_view(const ObjectID view_id) const {
  return _views.count(view_id) != 0;
}

void StorageManager::_add_prepared_plan(const ObjectID plan_id, const std::shared_ptr<PreparedPlan>& prepared_plan) {
  const auto inserted = _prepared_plans.emplace(plan_id, prepared_plan);
  Assert(inserted,
         "Cannot add prepared plan " + std::to_string(plan_id) + " - a prepared plan with the same ID already exists.");
}

void StorageManager::_drop_prepared_plan(const ObjectID plan_id) {
  const auto erased = _prepared_plans.erase(plan_id);
  Assert(erased, "Error deleting prepared plan. No such prepared plan with ID " + std::to_string(plan_id) + ".");
}

std::shared_ptr<PreparedPlan> StorageManager::get_prepared_plan(const ObjectID plan_id) const {
  auto accessor = decltype(_prepared_plans)::const_accessor{};
  _prepared_plans.find(accessor, plan_id);
  Assert(!accessor.empty(), "No such prepared plan with ID " + std::to_string(plan_id) + ".");
  return accessor->second;
}

bool StorageManager::has_prepared_plan(const ObjectID plan_id) const {
  return _prepared_plans.count(plan_id) != 0;
}

}  // namespace hyrise
