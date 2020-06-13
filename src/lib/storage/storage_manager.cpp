#include "storage_manager.hpp"

#include <boost/container/small_vector.hpp>
#include <fstream>
#include <memory>
#include <nlohmann/json.hpp>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "import_export/file_type.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/export.hpp"
#include "operators/print.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_iterate.hpp"
#include "utils/assert.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/timer.hpp"

namespace opossum {

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  const auto table_iter = _tables.find(name);
  const auto view_iter = _views.find(name);
  Assert(table_iter == _tables.end() || !table_iter->second,
         "Cannot add table " + name + " - a table with the same name already exists");
  Assert(view_iter == _views.end() || !view_iter->second,
         "Cannot add table " + name + " - a view with the same name already exists");

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    // We currently assume that all tables stored in the StorageManager are mutable and, as such, have MVCC data. This
    // way, we do not need to check query plans if they try to update immutable tables. However, this is not a hard
    // limitation and might be changed into more fine-grained assertions if the need arises.
    Assert(table->get_chunk(chunk_id)->has_mvcc_data(), "Table must have MVCC data.");
  }

  // Create table statistics and chunk pruning statistics for added table.

  table->set_table_statistics(TableStatistics::from_table(*table));
  generate_chunk_pruning_statistics(table);

  _tables[name] = std::move(table);
}

void StorageManager::drop_table(const std::string& name) {
  const auto table_iter = _tables.find(name);
  Assert(table_iter != _tables.end() && table_iter->second, "Error deleting table. No such table named '" + name + "'");

  // The concurrent_unordered_map does not support concurrency-safe erasure. Thus, we simply reset the table pointer.
  _tables[name] = nullptr;
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  const auto table_iter = _tables.find(name);
  Assert(table_iter != _tables.end(), "No such table named '" + name + "'");

  auto table = table_iter->second;
  Assert(table,
         "Nullptr found when accessing table named '" + name + "'. This can happen if a dropped table is accessed.");

  return table;
}

bool StorageManager::has_table(const std::string& name) const {
  const auto table_iter = _tables.find(name);
  return table_iter != _tables.end() && table_iter->second;
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names;
  table_names.reserve(_tables.size());

  for (const auto& table_item : _tables) {
    if (!table_item.second) continue;

    table_names.emplace_back(table_item.first);
  }

  return table_names;
}

const tbb::concurrent_unordered_map<std::string, std::shared_ptr<Table>>& StorageManager::tables() const {
  return _tables;
}

void StorageManager::add_view(const std::string& name, const std::shared_ptr<LQPView>& view) {
  const auto table_iter = _tables.find(name);
  const auto view_iter = _views.find(name);
  Assert(table_iter == _tables.end() || !table_iter->second,
         "Cannot add view " + name + " - a table with the same name already exists");
  Assert(view_iter == _views.end() || !view_iter->second,
         "Cannot add view " + name + " - a view with the same name already exists");

  _views[name] = view;
}

void StorageManager::drop_view(const std::string& name) {
  const auto view_iter = _views.find(name);
  Assert(view_iter != _views.end() && view_iter->second, "Error deleting view. No such view named '" + name + "'");

  _views[name] = nullptr;
}

std::shared_ptr<LQPView> StorageManager::get_view(const std::string& name) const {
  const auto view_iter = _views.find(name);
  Assert(view_iter != _views.end(), "No such view named '" + name + "'");

  const auto view = view_iter->second;
  Assert(view,
         "Nullptr found when accessing view named '" + name + "'. This can happen if a dropped view is accessed.");

  return view->deep_copy();
}

bool StorageManager::has_view(const std::string& name) const {
  const auto view_iter = _views.find(name);
  return view_iter != _views.end() && view_iter->second;
}

std::vector<std::string> StorageManager::view_names() const {
  std::vector<std::string> view_names;
  view_names.reserve(_views.size());

  for (const auto& view_item : _views) {
    if (!view_item.second) continue;

    view_names.emplace_back(view_item.first);
  }

  return view_names;
}

const tbb::concurrent_unordered_map<std::string, std::shared_ptr<LQPView>>& StorageManager::views() const {
  return _views;
}

void StorageManager::add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan) {
  const auto iter = _prepared_plans.find(name);
  Assert(iter == _prepared_plans.end() || !iter->second,
         "Cannot add prepared plan " + name + " - a prepared plan with the same name already exists");

  _prepared_plans[name] = prepared_plan;
}

std::shared_ptr<PreparedPlan> StorageManager::get_prepared_plan(const std::string& name) const {
  const auto iter = _prepared_plans.find(name);
  Assert(iter != _prepared_plans.end(), "No such prepared plan named '" + name + "'");

  auto prepared_plan = iter->second;
  Assert(prepared_plan, "Nullptr found when accessing prepared plan named '" + name +
                            "'. This can happen if a dropped prepared plan is accessed.");

  return prepared_plan;
}

bool StorageManager::has_prepared_plan(const std::string& name) const {
  const auto iter = _prepared_plans.find(name);
  return iter != _prepared_plans.end() && iter->second;
}

void StorageManager::drop_prepared_plan(const std::string& name) {
  const auto iter = _prepared_plans.find(name);
  Assert(iter != _prepared_plans.end() && iter->second,
         "Error deleting prepared plan. No such prepared plan named '" + name + "'");

  _prepared_plans[name] = nullptr;
}

const tbb::concurrent_unordered_map<std::string, std::shared_ptr<PreparedPlan>>& StorageManager::prepared_plans()
    const {
  return _prepared_plans;
}

void StorageManager::export_all_tables_as_csv(const std::string& path) {
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.reserve(_tables.size());

  for (const auto& table_item : _tables) {
    if (!table_item.second) continue;

    auto job_task = std::make_shared<JobTask>([table_item, &path]() {
      const auto& name = table_item.first;
      const auto& table = table_item.second;

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

void StorageManager::apply_partitioning() {
  const auto config_file = std::getenv("CLUSTERING") ? std::getenv("CLUSTERING") : "clustering.json";
  std::ifstream file(config_file);
  nlohmann::json json;
  file >> json;

  for (const auto& entry : json.items()) {
    const auto& table_name = entry.key();
    const auto& table = Hyrise::get().storage_manager.get_table(table_name);
    std::cout << "Clustering " << table_name << " according to " << config_file << std::endl;
    const auto& cluster_definition = entry.value();

    auto sort_column_definitions = std::vector<SortColumnDefinition>{};
    for (const auto& column_name : cluster_definition["cluster_columns"]) {
      const auto column_id = table->column_id_by_name(column_name);
      sort_column_definitions.emplace_back(SortColumnDefinition{column_id, SortMode::Ascending});
    }

    auto new_table = std::shared_ptr<const Table>{};
    {
      /**
       * CLUSTERING by using the sort operator
       */
      std::cout << "Clustering table " << table_name << std::flush;
      Timer timer;

      auto table_cluster_wrapper = std::make_shared<TableWrapper>(table);
      table_cluster_wrapper->execute();
      auto table_cluster_sort = std::make_shared<Sort>(table_cluster_wrapper, sort_column_definitions,
                                                    Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes);
      table_cluster_sort->execute();
      new_table = table_cluster_sort->get_output();

      std::cout << " - done (" << timer.lap_formatted() << ")" << std::endl;
    }

    auto sorted_table = std::make_shared<Table>(new_table->column_definitions(), TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);
    
    {
      if (cluster_definition.contains("sort_column")) {
        const auto sort_column_name = cluster_definition["sort_column"];
        /**
         * SORTING each cluster by the last cluster column
         */
        std::cout << "Sorting each chunk individually by " << sort_column_name << std::flush;
        Timer timer;

        const auto sort_column_id = table->column_id_by_name(sort_column_name);

        auto get_segments_of_chunk = [&](const std::shared_ptr<const Table>& input_table, ChunkID chunk_id){
          Segments segments{};
          for (auto column_id = ColumnID{0}; column_id < input_table->column_count(); ++column_id) {
            segments.emplace_back(input_table->get_chunk(chunk_id)->get_segment(column_id));
          }
          return segments;
        };

        
        const auto chunk_count = new_table->chunk_count();

        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          // create new single chunk and create a new table with that chunk
          auto new_chunk = std::make_shared<Chunk>(get_segments_of_chunk(new_table, chunk_id));
          std::vector<std::shared_ptr<Chunk>> single_chunk_to_sort_as_vector = {new_chunk};
          auto single_chunk_table = std::make_shared<Table>(new_table->column_definitions(), TableType::Data,
                                                            std::move(single_chunk_to_sort_as_vector), UseMvcc::No);

          // call sort operator on single-chunk table
          auto table_sort_wrapper = std::make_shared<TableWrapper>(single_chunk_table);
          table_sort_wrapper->execute();
          auto single_chunk_table_sort = std::make_shared<Sort>(table_sort_wrapper,
                                    std::vector<SortColumnDefinition>{SortColumnDefinition{sort_column_id,
                                    SortMode::Ascending}}, single_chunk_table->row_count(), Sort::ForceMaterialization::Yes);
          single_chunk_table_sort->execute();
          const auto immutable_single_chunk_sorted_table = single_chunk_table_sort->get_output();

          // add sorted chunk to output table
          auto mvcc_data = std::make_shared<MvccData>(immutable_single_chunk_sorted_table->row_count(), CommitID{0});
          sorted_table->append_chunk(get_segments_of_chunk(immutable_single_chunk_sorted_table, ChunkID{0}), mvcc_data);
          const auto& added_chunk = sorted_table->get_chunk(chunk_id);
          added_chunk->finalize();
          added_chunk->set_sorted_by(SortColumnDefinition(sort_column_id, SortMode::Ascending));
        }

        std::cout << " - done (" << timer.lap_formatted() << ")" << std::endl;
      }
    }

    {
      std::cout << "Applying dictionary encoding to new table" << std::flush;
      Timer timer;

      // Encode chunks in parallel, using `hardware_concurrency + 1` workers
      // Not using JobTasks here because we want parallelism even if the scheduler is disabled.
      auto next_chunk_id = std::atomic_uint{0};
      const auto thread_count =
          std::min(static_cast<uint>(sorted_table->chunk_count()), std::thread::hardware_concurrency() + 1);
      auto threads = std::vector<std::thread>{};
      threads.reserve(thread_count);

      for (auto thread_id = 0u; thread_id < thread_count; ++thread_id) {
        threads.emplace_back([&] {
          while (true) {
            auto my_chunk_id = next_chunk_id++;
            if (my_chunk_id >= sorted_table->chunk_count()) return;

            const auto chunk = sorted_table->get_chunk(ChunkID{my_chunk_id});
            ChunkEncoder::encode_chunk(chunk, sorted_table->column_data_types(),
                                       SegmentEncodingSpec{EncodingType::Dictionary});
          }
        });
      }

      for (auto& thread : threads) thread.join();

      std::cout << " - done (" << timer.lap_formatted() << ")" << std::endl;
    }

    {
      std::cout << "Generating statistics" << std::flush;
      Timer timer;
      drop_table(table_name);
      add_table(table_name, sorted_table);
      std::cout << " - done (" << timer.lap_formatted() << ")" << std::endl;
    }
  }
}

}  // namespace opossum
