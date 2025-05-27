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
#include "storage/storage_manager.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/meta_table_manager.hpp"

namespace hyrise {

namespace {
ObjectID add_object(const std::string& name, Catalog::ObjectMetadata& meta_data) {
  const auto object_id = meta_data.next_id++;
  if (object_id >= meta_data.names.size()) {
    meta_data.names.grow_to_at_least(object_id);
  }
  meta_data.names[object_id] = name;
  meta_data.ids[name] = object_id;
  return static_cast<ObjectID>(object_id);
}

std::vector<std::string_view> object_names(const Catalog::ObjectMetadata& meta_data) {
  auto names = std::vector<std::string_view>{};
  names.reserve(meta_data.ids.size());

  for (const auto& [name, object_id] : meta_data.ids) {
    if (object_id != INVALID_OBJECT_ID) {
      names.push_back(name);
    }
  }

  std::ranges::sort(names);
  return names;
}

std::unordered_map<std::string_view, ObjectID> object_ids(const Catalog::ObjectMetadata& meta_data) {
  auto result = std::unordered_map<std::string_view, ObjectID>{};

  for (const auto& [name, object_id] : meta_data.ids) {
    if (object_id != INVALID_OBJECT_ID) {
      result[name] = object_id;
    }
  }
  return result;
}

}  //  namespace

Catalog::ObjectMetadata& Catalog::ObjectMetadata::operator=(Catalog::ObjectMetadata&& other) noexcept {
  if (this != &other) {
    ids = std::move(other.ids);
    names = std::move(other.names);
    next_id = other.next_id.load();
  }
  return *this;
}

Catalog::ObjectMetadata::ObjectMetadata(Catalog::ObjectMetadata&& other) noexcept
    : ids{std::move(other.ids)}, names{std::move(other.names)}, next_id{other.next_id.load()} {}

void Catalog::add_table(const std::string& name, const std::shared_ptr<Table>& table) {
  const auto table_iter = _tables.ids.find(name);
  const auto view_iter = _views.ids.find(name);
  Assert(table_iter == _tables.ids.end() || table_iter->second == INVALID_OBJECT_ID,
         "Cannot add table " + name + " - a table with the same name already exists.");
  Assert(view_iter == _views.ids.end() || view_iter->second == INVALID_OBJECT_ID,
         "Cannot add table " + name + " - a view with the same name already exists.");
  const auto table_id = add_object(name, _tables);
  Hyrise::get().storage_manager._add_table(table_id, table);
}

void Catalog::drop_table(ObjectID table_id) {
  Assert(table_id < _tables.names.size(), "ObjectID " + std::to_string(table_id) + " out of range.");
  drop_table(_tables.names[table_id]);
}

void Catalog::drop_table(const std::string& name) {
  const auto iter = _tables.ids.find(name);
  Assert(iter != _tables.ids.end() && iter->second != INVALID_OBJECT_ID,
         "Error deleting table. No such table named '" + name + "'.");

  // The `concurrent_unordered_map` does not support concurrency-safe erasure. Thus, we simply reset the table ID.
  _tables.ids[name] = INVALID_OBJECT_ID;
  Hyrise::get().storage_manager._drop_table(iter->second);
}

ObjectID Catalog::table_id(const std::string& name) const {
  const auto iter = _tables.ids.find(name);
  return iter == _tables.ids.end() ? INVALID_OBJECT_ID : iter->second;
}

const std::string& Catalog::table_name(const ObjectID table_id) const {
  Assert(table_id < _tables.names.size(), "ObjectID " + std::to_string(table_id) + " out of range.");
  return _tables.names[table_id];
}

std::vector<std::string_view> Catalog::table_names() const {
  return object_names(_tables);
}

std::unordered_map<std::string_view, ObjectID> Catalog::table_ids() const {
  return object_ids(_tables);
}

void Catalog::add_view(const std::string& name, const std::shared_ptr<LQPView>& view) {
  const auto table_iter = _tables.ids.find(name);
  const auto view_iter = _views.ids.find(name);
  Assert(table_iter == _tables.ids.end() || table_iter->second == INVALID_OBJECT_ID,
         "Cannot add view " + name + " - a table with the same name already exists.");
  Assert(view_iter == _views.ids.end() || view_iter->second == INVALID_OBJECT_ID,
         "Cannot add view " + name + " - a view with the same name already exists.");
  const auto view_id = add_object(name, _views);
  Hyrise::get().storage_manager._add_view(view_id, view);
}

void Catalog::drop_view(ObjectID view_id) {
  Assert(view_id < _views.names.size(), "ObjectID " + std::to_string(view_id) + " out of range.");
  drop_view(_views.names[view_id]);
}

void Catalog::drop_view(const std::string& name) {
  const auto iter = _views.ids.find(name);
  Assert(iter != _views.ids.end() && iter->second != INVALID_OBJECT_ID,
         "Error deleting view. No such view named '" + name + "'.");

  // The `concurrent_unordered_map` does not support concurrency-safe erasure. Thus, we simply reset the table ID.
  _views.ids[name] = INVALID_OBJECT_ID;
  Hyrise::get().storage_manager._drop_view(iter->second);
}

ObjectID Catalog::view_id(const std::string& name) const {
  const auto iter = _views.ids.find(name);
  return iter == _views.ids.end() ? INVALID_OBJECT_ID : iter->second;
}

const std::string& Catalog::view_name(const ObjectID view_id) const {
  Assert(view_id < _views.names.size(), "ObjectID " + std::to_string(view_id) + " out of range.");
  return _views.names[view_id];
}

std::vector<std::string_view> Catalog::view_names() const {
  return object_names(_views);
}

std::unordered_map<std::string_view, ObjectID> Catalog::view_ids() const {
  return object_ids(_tables);
}

void Catalog::add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan) {
  const auto iter = _prepared_plans.ids.find(name);
  Assert(iter == _prepared_plans.ids.end() || iter->second == INVALID_OBJECT_ID,
         "Cannot add prepared plan '" + name + "' - a prepared plan with the same name already exists.");
  const auto plan_id = add_object(name, _prepared_plans);
  Hyrise::get().storage_manager._add_prepared_plan(plan_id, prepared_plan);
}

void Catalog::drop_prepared_plan(ObjectID plan_id) {
  Assert(plan_id < _prepared_plans.names.size(), "ObjectID " + std::to_string(plan_id) + " out of range.");
  drop_prepared_plan(_prepared_plans.names[plan_id]);
}

void Catalog::drop_prepared_plan(const std::string& name) {
  const auto iter = _prepared_plans.ids.find(name);
  Assert(iter != _prepared_plans.ids.end() && iter->second != INVALID_OBJECT_ID,
         "Error deleting view. No such prepared plan named '" + name + "'.");

  // The `concurrent_unordered_map` does not support concurrency-safe erasure. Thus, we simply reset the table ID.
  _prepared_plans.ids[name] = INVALID_OBJECT_ID;
  Hyrise::get().storage_manager._drop_prepared_plan(iter->second);
}

ObjectID Catalog::prepared_plan_id(const std::string& name) const {
  const auto iter = _prepared_plans.ids.find(name);
  return iter == _prepared_plans.ids.end() ? INVALID_OBJECT_ID : iter->second;
}

const std::string& Catalog::prepared_plan_name(const ObjectID plan_id) const {
  Assert(plan_id < _prepared_plans.names.size(), "ObjectID " + std::to_string(plan_id) + " out of range.");
  return _prepared_plans.names[plan_id];
}

std::unordered_map<std::string_view, ObjectID> Catalog::prepared_plan_ids() const {
  return object_ids(_prepared_plans);
}

}  // namespace hyrise
