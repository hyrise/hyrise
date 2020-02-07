#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "import_export/file_type.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/export.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"
#include "utils/meta_table_manager.hpp"

namespace opossum {

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  Assert(_tables.find(name) == _tables.end(), "A table with the name " + name + " already exists");
  Assert(_views.find(name) == _views.end(), "Cannot add table " + name + " - a view with the same name already exists");

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    // We currently assume that all tables stored in the StorageManager are mutable and, as such, have MVCC data. This
    // way, we do not need to check query plans if they try to update immutable tables. However, this is not a hard
    // limitation and might be changed into more fine-grained assertions if the need arises.
    Assert(table->get_chunk(chunk_id)->has_mvcc_data(), "Table must have MVCC data.");
  }

  table->set_table_statistics(TableStatistics::from_table(*table));
  _tables.emplace(name, std::move(table));
}

void StorageManager::drop_table(const std::string& name) {
  const auto num_deleted = _tables.erase(name);
  Assert(num_deleted == 1, "Error deleting table " + name + ": _erase() returned " + std::to_string(num_deleted) + ".");
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  if (MetaTableManager::is_meta_table_name(name)) {
    return Hyrise::get().meta_table_manager.generate_table(name.substr(MetaTableManager::META_PREFIX.size()));
  }

  const auto iter = _tables.find(name);
  Assert(iter != _tables.end(), "No such table named '" + name + "'");

  return iter->second;
}

bool StorageManager::has_table(const std::string& name) const {
  if (MetaTableManager::is_meta_table_name(name)) {
    const auto& meta_table_names = Hyrise::get().meta_table_manager.table_names();
    return std::binary_search(meta_table_names.begin(), meta_table_names.end(),
                              name.substr(MetaTableManager::META_PREFIX.size()));
  }
  return _tables.count(name);
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names;
  table_names.reserve(_tables.size());

  for (const auto& table_item : _tables) {
    table_names.emplace_back(table_item.first);
  }

  return table_names;
}

const std::map<std::string, std::shared_ptr<Table>>& StorageManager::tables() const { return _tables; }

void StorageManager::add_view(const std::string& name, const std::shared_ptr<LQPView>& view) {
  std::unique_lock lock(*_view_mutex);

  Assert(_tables.find(name) == _tables.end(),
         "Cannot add view " + name + " - a table with the same name already exists");
  Assert(_views.find(name) == _views.end(), "A view with the name " + name + " already exists");

  _views.emplace(name, view);
}

void StorageManager::drop_view(const std::string& name) {
  std::unique_lock lock(*_view_mutex);

  const auto num_deleted = _views.erase(name);
  Assert(num_deleted == 1, "Error deleting view " + name + ": _erase() returned " + std::to_string(num_deleted) + ".");
}

std::shared_ptr<LQPView> StorageManager::get_view(const std::string& name) const {
  std::shared_lock lock(*_view_mutex);

  const auto iter = _views.find(name);
  Assert(iter != _views.end(), "No such view named '" + name + "'");

  return iter->second->deep_copy();
}

bool StorageManager::has_view(const std::string& name) const {
  std::shared_lock lock(*_view_mutex);

  return _views.count(name);
}

std::vector<std::string> StorageManager::view_names() const {
  std::shared_lock lock(*_view_mutex);

  std::vector<std::string> view_names;
  view_names.reserve(_views.size());

  for (const auto& view_item : _views) {
    view_names.emplace_back(view_item.first);
  }

  return view_names;
}

const std::map<std::string, std::shared_ptr<LQPView>>& StorageManager::views() const { return _views; }

void StorageManager::add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan) {
  Assert(_prepared_plans.find(name) == _prepared_plans.end(),
         "Cannot add prepared plan " + name + " - a prepared plan with the same name already exists");

  _prepared_plans.emplace(name, prepared_plan);
}

std::shared_ptr<PreparedPlan> StorageManager::get_prepared_plan(const std::string& name) const {
  const auto iter = _prepared_plans.find(name);
  Assert(iter != _prepared_plans.end(), "No such prepared plan named '" + name + "'");

  return iter->second;
}

bool StorageManager::has_prepared_plan(const std::string& name) const {
  return _prepared_plans.find(name) != _prepared_plans.end();
}

void StorageManager::drop_prepared_plan(const std::string& name) {
  const auto iter = _prepared_plans.find(name);
  Assert(iter != _prepared_plans.end(), "No such prepared plan named '" + name + "'");

  _prepared_plans.erase(iter);
}

const std::map<std::string, std::shared_ptr<PreparedPlan>>& StorageManager::prepared_plans() const {
  return _prepared_plans;
}

void StorageManager::export_all_tables_as_csv(const std::string& path) {
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.reserve(_tables.size());

  for (auto& pair : _tables) {
    auto job_task = std::make_shared<JobTask>([pair, &path]() {
      const auto& name = pair.first;
      auto& table = pair.second;

      auto table_wrapper = std::make_shared<TableWrapper>(table);
      table_wrapper->execute();

      auto export_csv = std::make_shared<Export>(table_wrapper, path + "/" + name + ".csv", FileType::Csv);  // NOLINT
      export_csv->execute();
    });
    tasks.push_back(job_task);
    job_task->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(tasks);
}

std::ostream& operator<<(std::ostream& stream, const StorageManager& storage_manager) {
  stream << "==================" << std::endl;
  stream << "===== Tables =====" << std::endl << std::endl;

  for (auto const& table : storage_manager.tables()) {
    stream << "==== table >> " << table.first << " <<";
    stream << " (" << table.second->column_count() << " columns, " << table.second->row_count() << " rows in "
           << table.second->chunk_count() << " chunks)";
    stream << std::endl;
  }

  stream << "==================" << std::endl;
  stream << "===== Views ======" << std::endl << std::endl;

  for (auto const& view : storage_manager.views()) {
    stream << "==== view >> " << view.first << " <<";
    stream << std::endl;
  }

  stream << "==================" << std::endl;
  stream << "= PreparedPlans ==" << std::endl << std::endl;

  for (auto const& prepared_plan : storage_manager.prepared_plans()) {
    stream << "==== prepared plan >> " << prepared_plan.first << " <<";
    stream << std::endl;
  }

  return stream;
}

}  // namespace opossum
