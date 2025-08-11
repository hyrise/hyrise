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
  {
    auto accessor = tbb::concurrent_hash_map<std::string, ObjectID>::accessor{};
    const auto emplaced = meta_data.ids.emplace(accessor, name, object_id);
    if (!emplaced) {
      Assert(emplaced || accessor->second == INVALID_OBJECT_ID,
        "Cannot add object " + name + " - an object with the same name already exists.");
      accessor->second = object_id;

    }
  }
  // IDs are unique, so we do not have to take care of updating probably existing entries.
  meta_data.names.emplace(object_id, name);
  return object_id;
}

ObjectID drop_object(const std::string& name, Catalog::ObjectMetadata& meta_data) {
  auto accessor = tbb::concurrent_hash_map<std::string, ObjectID>::accessor{};
  meta_data.ids.find(accessor, name);
  Assert(!accessor.empty(), "Error deleting object. No such object named '" + name + "'.");
  const auto object_id = accessor->second;
  accessor->second = INVALID_OBJECT_ID;
  return object_id;
}

ObjectID object_id(const std::string& name, const tbb::concurrent_hash_map<std::string, ObjectID>& map) {
  auto accessor = tbb::concurrent_hash_map<std::string, ObjectID>::const_accessor{};
  map.find(accessor, name);
  return accessor.empty() ? INVALID_OBJECT_ID : accessor->second;
}

const std::string object_name(const ObjectID object_id, const tbb::concurrent_hash_map<ObjectID, std::string>& map) {
  auto accessor = tbb::concurrent_hash_map<ObjectID, std::string>::const_accessor{};
  map.find(accessor, object_id);
  Assert(!accessor.empty(), "Unknown object with ID " + std::to_string(object_id) + ".");
  return accessor->second;
}

std::unordered_map<std::string, ObjectID> object_ids(const Catalog::ObjectMetadata& meta_data) {
  auto result = std::unordered_map<std::string, ObjectID>{};

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

std::pair<ObjectType, ObjectID> Catalog::resolve_object(const std::string& name) {
  const auto table_id = this->table_id(name);
  if (table_id != INVALID_OBJECT_ID) {
    return {ObjectType::Table, table_id};
  }

  const auto view_id = this->view_id(name);
  if (view_id != INVALID_OBJECT_ID) {
    return {ObjectType::View, view_id};
  }

  const auto plan_id = this->prepared_plan_id(name);
  if (plan_id != INVALID_OBJECT_ID) {
    return {ObjectType::PreparedPlan, plan_id};
  }

  Fail("Unknown object: '" + name + "'");
}

ObjectID Catalog::add_table(const std::string& name, const std::shared_ptr<Table>& table) {
  auto accessor = tbb::concurrent_hash_map<std::string, ObjectID>::const_accessor{};
  _views.ids.find(accessor, name);
  Assert(accessor.empty() || accessor->second == INVALID_OBJECT_ID,
         "Cannot add table " + name + " - a view with the same name already exists.");

  const auto table_id = add_object(name, _tables);
  Hyrise::get().storage_manager._add_table(table_id, table);
  return table_id;
}

void Catalog::drop_table(ObjectID table_id) {
  drop_table(object_name(table_id, _tables.names));
}

void Catalog::drop_table(const std::string& name) {
  const auto table_id = drop_object(name, _tables);
  Hyrise::get().storage_manager._drop_table(table_id);
}

bool Catalog::has_table(const std::string& name) const {
  return table_id(name) != INVALID_OBJECT_ID;
}

ObjectID Catalog::table_id(const std::string& name) const {
  return object_id(name, _tables.ids);
}

std::string Catalog::table_name(const ObjectID table_id) const {
  return object_name(table_id, _tables.names);
}

std::vector<std::string> Catalog::table_names() const {
  auto names = std::vector<std::string>{};
  names.reserve(_tables.ids.size());

  for (const auto& [name, object_id] : _tables.ids) {
    if (object_id != INVALID_OBJECT_ID) {
      names.push_back(name);
    }
  }

  std::ranges::sort(names);
  return names;
}

std::unordered_map<std::string, ObjectID> Catalog::table_ids() const {
  return object_ids(_tables);
}

std::unordered_map<std::string, std::shared_ptr<Table>> Catalog::tables() const {
  auto tables = std::unordered_map<std::string, std::shared_ptr<Table>>{};

  for (const auto& [name, table_id] : _tables.ids) {
    if (table_id != INVALID_OBJECT_ID) {
      tables[name] = Hyrise::get().storage_manager.get_table(table_id);
    }
  }
  return tables;
}

ObjectID Catalog::add_view(const std::string& name, const std::shared_ptr<LQPView>& view) {
  auto accessor = tbb::concurrent_hash_map<std::string, ObjectID>::const_accessor{};
  _tables.ids.find(accessor, name);
  Assert(accessor.empty() || accessor->second == INVALID_OBJECT_ID,
         "Cannot add view " + name + " - a table with the same name already exists.");
  const auto view_id = add_object(name, _views);
  Hyrise::get().storage_manager._add_view(view_id, view);
  return view_id;
}

void Catalog::drop_view(ObjectID view_id) {
  drop_view(object_name(view_id, _views.names));
}

void Catalog::drop_view(const std::string& name) {
  const auto view_id = drop_object(name, _views);
  Hyrise::get().storage_manager._drop_view(view_id);
}

bool Catalog::has_view(const std::string& name) const {
  return view_id(name) != INVALID_OBJECT_ID;
}

ObjectID Catalog::view_id(const std::string& name) const {
  return object_id(name, _views.ids);
}

std::string Catalog::view_name(const ObjectID view_id) const {
  return object_name(view_id, _views.names);
}

ObjectID Catalog::add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan) {
  const auto plan_id = add_object(name, _prepared_plans);
  Hyrise::get().storage_manager._add_prepared_plan(plan_id, prepared_plan);
  return plan_id;
}

void Catalog::drop_prepared_plan(ObjectID plan_id) {
  drop_view(object_name(plan_id, _prepared_plans.names));
}

void Catalog::drop_prepared_plan(const std::string& name) {
  const auto plan_id = drop_object(name, _prepared_plans);
  Hyrise::get().storage_manager._drop_prepared_plan(plan_id);
}

bool Catalog::has_prepared_plan(const std::string& name) const {
  return prepared_plan_id(name) != INVALID_OBJECT_ID;
}

ObjectID Catalog::prepared_plan_id(const std::string& name) const {
  return object_id(name, _prepared_plans.ids);
}

std::string Catalog::prepared_plan_name(const ObjectID plan_id) const {
  return object_name(plan_id, _prepared_plans.names);
}

}  // namespace hyrise
