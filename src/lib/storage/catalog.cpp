#include "catalog.hpp"

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
#include "storage/lqp_view.hpp"
#include "storage/prepared_plan.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

namespace {

ObjectID add_object(const std::string& name, Catalog::ObjectMetadata& meta_data) {
  const auto object_id = static_cast<ObjectID>(meta_data.next_id++);
  const auto emplaced = meta_data.ids.emplace(name, object_id);
  Assert(emplaced, "Cannot add object " + name + " - an object with the same name already exists.");
  // IDs are unique, so we do not have to take care of already existing entries.
  meta_data.names.emplace(object_id, name);
  return object_id;
}

ObjectID drop_object(const std::string& name, Catalog::ObjectMetadata& meta_data) {
  auto accessor = tbb::concurrent_hash_map<std::string, ObjectID>::accessor{};
  meta_data.ids.find(accessor, name);
  Assert(!accessor.empty(), "Error deleting object. No such object named '" + name + "'.");
  const auto object_id = accessor->second;
  meta_data.ids.erase(accessor);
  return object_id;
}

ObjectID object_id(const std::string& name, const Catalog::ObjectMetadata& meta_data) {
  auto accessor = decltype(meta_data.ids)::const_accessor{};
  meta_data.ids.find(accessor, name);
  return accessor.empty() ? INVALID_OBJECT_ID : accessor->second;
}

std::string object_name(const ObjectID object_id, const Catalog::ObjectMetadata& meta_data) {
  auto accessor = decltype(meta_data.names)::const_accessor{};
  meta_data.names.find(accessor, object_id);
  Assert(!accessor.empty(), "Unknown object with ID " + std::to_string(object_id) + ".");
  return accessor->second;
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

std::pair<ObjectType, ObjectID> Catalog::resolve_stored_object(const std::string& name) {
  const auto table_id = this->table_id(name);
  if (table_id != INVALID_OBJECT_ID) {
    return {ObjectType::Table, table_id};
  }

  const auto view_id = this->view_id(name);
  if (view_id != INVALID_OBJECT_ID) {
    return {ObjectType::View, view_id};
  }

  return {ObjectType::Unknown, INVALID_OBJECT_ID};
}

ObjectID Catalog::add_table(const std::string& name, const std::shared_ptr<Table>& table) {
  Assert(_views.ids.count(name) == 0, "Cannot add table " + name + " - a view with the same name already exists.");

  const auto table_id = add_object(name, _tables);
  Hyrise::get().storage_manager._add_table(table_id, table);
  return table_id;
}

void Catalog::drop_table(ObjectID table_id) {
  drop_table(object_name(table_id, _tables));
}

void Catalog::drop_table(const std::string& name) {
  const auto table_id = drop_object(name, _tables);
  Hyrise::get().storage_manager._drop_table(table_id);
}

bool Catalog::has_table(const std::string& name) const {
  return table_id(name) != INVALID_OBJECT_ID;
}

ObjectID Catalog::table_id(const std::string& name) const {
  return object_id(name, _tables);
}

std::string Catalog::table_name(const ObjectID table_id) const {
  return object_name(table_id, _tables);
}

std::vector<std::string> Catalog::table_names() const {
  auto names = std::vector<std::string>{};
  names.reserve(_tables.ids.size());

  for (const auto& [name, _] : _tables.ids) {
    names.push_back(name);
  }

  std::ranges::sort(names);
  return names;
}

std::unordered_map<std::string, ObjectID> Catalog::table_ids() const {
  auto result = std::unordered_map<std::string, ObjectID>{};

  for (const auto& [name, table_id] : _tables.ids) {
    result[name] = table_id;
  }
  return result;
}

std::unordered_map<std::string, std::shared_ptr<Table>> Catalog::tables() const {
  auto tables = std::unordered_map<std::string, std::shared_ptr<Table>>{};

  for (const auto& [name, table_id] : _tables.ids) {
    tables[name] = Hyrise::get().storage_manager.get_table(table_id);
  }
  return tables;
}

ObjectID Catalog::add_view(const std::string& name, const std::shared_ptr<LQPView>& view) {
  Assert(_tables.ids.count(name) == 0, "Cannot add view " + name + " - a table with the same name already exists.");
  const auto view_id = add_object(name, _views);
  Hyrise::get().storage_manager._add_view(view_id, view);
  return view_id;
}

void Catalog::drop_view(ObjectID view_id) {
  drop_view(object_name(view_id, _views));
}

void Catalog::drop_view(const std::string& name) {
  const auto view_id = drop_object(name, _views);
  Hyrise::get().storage_manager._drop_view(view_id);
}

bool Catalog::has_view(const std::string& name) const {
  return view_id(name) != INVALID_OBJECT_ID;
}

ObjectID Catalog::view_id(const std::string& name) const {
  return object_id(name, _views);
}

std::string Catalog::view_name(const ObjectID view_id) const {
  return object_name(view_id, _views);
}

ObjectID Catalog::add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan) {
  const auto plan_id = add_object(name, _prepared_plans);
  Hyrise::get().storage_manager._add_prepared_plan(plan_id, prepared_plan);
  return plan_id;
}

void Catalog::drop_prepared_plan(ObjectID plan_id) {
  drop_prepared_plan(object_name(plan_id, _prepared_plans));
}

void Catalog::drop_prepared_plan(const std::string& name) {
  const auto plan_id = drop_object(name, _prepared_plans);
  Hyrise::get().storage_manager._drop_prepared_plan(plan_id);
}

bool Catalog::has_prepared_plan(const std::string& name) const {
  return prepared_plan_id(name) != INVALID_OBJECT_ID;
}

ObjectID Catalog::prepared_plan_id(const std::string& name) const {
  return object_id(name, _prepared_plans);
}

std::string Catalog::prepared_plan_name(const ObjectID plan_id) const {
  return object_name(plan_id, _prepared_plans);
}

}  // namespace hyrise
