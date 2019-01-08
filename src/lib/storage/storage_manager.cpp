#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/export_csv.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/generate_table_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  Assert(_tables.find(name) == _tables.end(), "A table with the name " + name + " already exists");
  Assert(_views.find(name) == _views.end(), "Cannot add table " + name + " - a view with the same name already exists");

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    Assert(table->get_chunk(chunk_id)->has_mvcc_data(), "Table must have MVCC data.");
  }

  table->set_table_statistics(std::make_shared<TableStatistics>(generate_table_statistics(*table)));
  _tables.emplace(name, std::move(table));
}

void StorageManager::drop_table(const std::string& name) {
  const auto num_deleted = _tables.erase(name);
  Assert(num_deleted == 1, "Error deleting table " + name + ": _erase() returned " + std::to_string(num_deleted) + ".");
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  const auto iter = _tables.find(name);
  Assert(iter != _tables.end(), "No such table named '" + name + "'");

  return iter->second;
}

bool StorageManager::has_table(const std::string& name) const { return _tables.count(name); }

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
  Assert(_tables.find(name) == _tables.end(),
         "Cannot add view " + name + " - a table with the same name already exists");
  Assert(_views.find(name) == _views.end(), "A view with the name " + name + " already exists");

  _views.emplace(name, view);
}

void StorageManager::drop_view(const std::string& name) {
  const auto num_deleted = _views.erase(name);
  Assert(num_deleted == 1, "Error deleting view " + name + ": _erase() returned " + std::to_string(num_deleted) + ".");
}

std::shared_ptr<LQPView> StorageManager::get_view(const std::string& name) const {
  const auto iter = _views.find(name);
  Assert(iter != _views.end(), "No such view named '" + name + "'");

  return iter->second->deep_copy();
}

bool StorageManager::has_view(const std::string& name) const { return _views.count(name); }

std::vector<std::string> StorageManager::view_names() const {
  std::vector<std::string> view_names;
  view_names.reserve(_views.size());

  for (const auto& view_item : _views) {
    view_names.emplace_back(view_item.first);
  }

  return view_names;
}

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

void StorageManager::print(std::ostream& out) const {
  out << "==================" << std::endl;
  out << "===== Tables =====" << std::endl << std::endl;

  for (auto const& table : _tables) {
    out << "==== table >> " << table.first << " <<";
    out << " (" << table.second->column_count() << " columns, " << table.second->row_count() << " rows in "
        << table.second->chunk_count() << " chunks)";
    out << std::endl;
  }

  out << "==================" << std::endl;
  out << "===== Views ======" << std::endl << std::endl;

  for (auto const& view : _views) {
    out << "==== view >> " << view.first << " <<";
    out << std::endl;
  }

  out << "==================" << std::endl;
  out << "= PreparedPlans ==" << std::endl << std::endl;

  for (auto const& prepared_plan : _prepared_plans) {
    out << "==== prepared plan >> " << prepared_plan.first << " <<";
    out << std::endl;
  }
}

void StorageManager::reset() { get() = StorageManager(); }

void StorageManager::export_all_tables_as_csv(const std::string& path) {
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.reserve(_tables.size());

  for (auto& pair : _tables) {
    auto job_task = std::make_shared<JobTask>([pair, &path]() {
      const auto& name = pair.first;
      auto& table = pair.second;

      auto table_wrapper = std::make_shared<TableWrapper>(table);
      table_wrapper->execute();

      auto export_csv = std::make_shared<ExportCsv>(table_wrapper, path + "/" + name + ".csv");  // NOLINT
      export_csv->execute();
    });
    tasks.push_back(job_task);
    job_task->schedule();
  }

  CurrentScheduler::wait_for_tasks(tasks);
}

}  // namespace opossum
