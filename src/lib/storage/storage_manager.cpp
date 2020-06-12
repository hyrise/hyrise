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
    const auto& dimensions = entry.value();

    auto partition_by_row_idx = std::vector<size_t>(table->row_count());
    auto row_id_by_row_idx = std::vector<RowID>(table->row_count());

    auto total_num_partitions = size_t{1};

    for (auto dimension_id = size_t{0}; dimension_id < dimensions.size(); ++dimension_id) {
      const auto& dimension = dimensions[dimension_id];
      const auto partition_count = static_cast<size_t>(dimension["clusters"]);

      bool partition_by_values;
      if (dimension["mode"] == "size") {
        partition_by_values = false;
      } else if (dimension["mode"] == "values") {
        std::cout << "WARNING: use 'values' clustering with care. It's not supposed to be used for the TuK exercise." << std::endl;
        partition_by_values = true;
      } else {
        Fail("Unknown mode");
      }

      std::cout << "\tCalculating boundaries for " << dimension["column_name"] << std::endl;
      Timer timer;

      const auto column_id = table->column_id_by_name(dimension["column_name"]);
      resolve_data_type(table->column_data_type(column_id), [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        auto materialized = std::vector<std::pair<ColumnDataType, size_t>>{};
        materialized.reserve(table->row_count());

        {
          auto row_idx = size_t{0};
          for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
            const auto& chunk = table->get_chunk(chunk_id);
            const auto& segment = chunk->get_segment(column_id);

            segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
              Assert(!position.is_null(), "Clustering on NULL values not yet supported");
              materialized.emplace_back(std::pair<ColumnDataType, size_t>{position.value(), row_idx});
              row_id_by_row_idx[row_idx] = RowID{chunk_id, position.chunk_offset()};

              ++row_idx;
            });
          }
        }

        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(materialized.begin(), materialized.end(), g);
        std::sort(materialized.begin(), materialized.end());

        auto distinct_values = std::vector<ColumnDataType>{};

        if (partition_count == 1) {
          std::cout << "WARNING: be cautious when using a cluster split count of 1" << std::endl;
        }

        if (partition_by_values) {
          distinct_values.resize(materialized.size());
          for (auto i = size_t{0}; i < materialized.size(); ++i) {
            distinct_values[i] = materialized[i].first;
          }
          distinct_values.erase(std::unique(distinct_values.begin(), distinct_values.end()), distinct_values.end());
          Assert(partition_count <= distinct_values.size(), "More clusters requested than distinct values found");
        } else {
          Assert(partition_count <= materialized.size(), "More clusters requested than rows in table");
        }

        total_num_partitions *= partition_count;

        auto materialized_idx = size_t{0};
        for (auto partition_id = size_t{0}; partition_id < partition_count; ++partition_id) {
          ColumnDataType first_value_in_next_partition{};
          if (partition_by_values) {
            first_value_in_next_partition = distinct_values[std::min(
                (partition_id + 1) * distinct_values.size() / partition_count, distinct_values.size() - 1)];
          }

          auto current_partition_size = size_t{0};

          if (partition_by_values) {
            std::cout << "\t\tfrom value '" << materialized[materialized_idx].first << "' to value '";
          } else {
            std::cout << "\t\tfrom row " << materialized_idx << " to row ";
          }

          while (materialized_idx < materialized.size()) {
            const auto& [value, row_idx] = materialized[materialized_idx];

            // Shift existing partitions
            partition_by_row_idx[row_idx] *= partition_count;

            // Set partition of this
            partition_by_row_idx[row_idx] += partition_id;

            ++materialized_idx;
            ++current_partition_size;

            if (partition_by_values && materialized[materialized_idx].first == first_value_in_next_partition &&
                (partition_id < partition_count - 1 || materialized_idx == materialized.size() - 1)) {
              std::cout << materialized[materialized_idx - 1].first << "' (" << current_partition_size << " rows)"
                        << std::endl;
              first_value_in_next_partition = distinct_values[std::min(
                  (partition_id + 1) * distinct_values.size() / partition_count, distinct_values.size() - 1)];
              break;
            }

            if (!partition_by_values && ((current_partition_size >= materialized.size() / partition_count &&
                                          partition_id != partition_count - 1) ||
                                         (materialized_idx == materialized.size() - 1))) {
              std::cout << materialized_idx << " (" << current_partition_size << " rows)" << std::endl;
              break;
            }
          }
        }
      });

      std::cout << "\t\tdone (" << timer.lap_formatted() << ")" << std::endl;
    }

    // Write segments
    auto segments_by_partition = std::vector<Segments>(total_num_partitions, Segments(table->column_count()));
    {
      std::cout << "\tWriting clustered columns in parallel" << std::flush;
      Timer timer;

      auto threads = std::vector<std::thread>{};
      for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
        threads.emplace_back(std::thread([&, column_id] {
          resolve_data_type(table->column_data_type(column_id), [&](auto type) {
            using ColumnDataType = typename decltype(type)::type;

            auto original_dictionary_segments =
                std::vector<std::shared_ptr<const DictionarySegment<ColumnDataType>>>(table->chunk_count());
            for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
              const auto& original_chunk = table->get_chunk(chunk_id);
              const auto& original_segment = original_chunk->get_segment(column_id);
              const auto& original_dictionary_segment =
                  std::static_pointer_cast<const DictionarySegment<ColumnDataType>>(original_segment);

              original_dictionary_segments[chunk_id] = original_dictionary_segment;
            }

            auto values_by_partition = std::vector<pmr_vector<ColumnDataType>>(total_num_partitions);
            for (auto& values : values_by_partition) {
              values.reserve(table->row_count() / std::max(1ul, (total_num_partitions - 1)));
            }

            const auto row_count = table->row_count();
            for (auto row_idx = size_t{0}; row_idx < row_count; ++row_idx) {
              const auto [chunk_id, chunk_offset] = row_id_by_row_idx[row_idx];
              const auto partition_id = partition_by_row_idx[row_idx];

              const auto& original_dictionary_segment = original_dictionary_segments[chunk_id];

              values_by_partition[partition_id].emplace_back(
                  *original_dictionary_segment->get_typed_value(chunk_offset));
            }

            for (auto partition_id = size_t{0}; partition_id < total_num_partitions; ++partition_id) {
              auto value_segment =
                  std::make_shared<ValueSegment<ColumnDataType>>(std::move(values_by_partition[partition_id]));
              segments_by_partition[partition_id][column_id] = value_segment;
            }
          });
        }));
      }
      for (auto& thread : threads) thread.join();

      std::cout << " - done (" << timer.lap_formatted() << ")" << std::endl;
    }

    // Write new table
    auto new_table = std::make_shared<Table>(table->column_definitions(), TableType::Data, std::nullopt, UseMvcc::Yes);
    for (auto partition_id = size_t{0}; partition_id < total_num_partitions; ++partition_id) {
      const auto& segments = segments_by_partition[partition_id];
      if (segments[0]->size() == 0) continue;
      // Note that this makes all rows that have been deleted visible again
      auto mvcc_data = std::make_shared<MvccData>(segments[0]->size(), CommitID{0});
      new_table->append_chunk(segments, mvcc_data);
      new_table->last_chunk()->finalize();
    }

    {
      /**
       * SORTING each cluster by the last cluster column
       */
      std::cout << "Sorting each chunk individually by " << dimensions[dimensions.size() - 1]["column_name"] << std::flush;
      Timer timer;

      const auto sort_column_id = table->column_id_by_name(dimensions[dimensions.size() - 1]["column_name"]);

      auto get_segments_of_chunk = [&](const std::shared_ptr<const Table>& input_table, ChunkID chunk_id){
        Segments segments{};
        for (auto column_id = ColumnID{0}; column_id < input_table->column_count(); ++column_id) {
          segments.emplace_back(input_table->get_chunk(chunk_id)->get_segment(column_id));
        }
        return segments;
      };

      auto sorted_table = std::make_shared<Table>(new_table->column_definitions(), TableType::Data, Chunk::DEFAULT_SIZE * 17, UseMvcc::Yes);
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

      new_table = sorted_table;

      std::cout << " - done (" << timer.lap_formatted() << ")" << std::endl;
    }

    {
      std::cout << "Applying dictionary encoding to new table" << std::flush;
      Timer timer;

      // Encode chunks in parallel, using `hardware_concurrency + 1` workers
      // Not using JobTasks here because we want parallelism even if the scheduler is disabled.
      auto next_chunk_id = std::atomic_uint{0};
      const auto thread_count =
          std::min(static_cast<uint>(new_table->chunk_count()), std::thread::hardware_concurrency() + 1);
      auto threads = std::vector<std::thread>{};
      threads.reserve(thread_count);

      for (auto thread_id = 0u; thread_id < thread_count; ++thread_id) {
        threads.emplace_back([&] {
          while (true) {
            auto my_chunk_id = next_chunk_id++;
            if (my_chunk_id >= new_table->chunk_count()) return;

            const auto chunk = new_table->get_chunk(ChunkID{my_chunk_id});
            ChunkEncoder::encode_chunk(chunk, new_table->column_data_types(),
                                       SegmentEncodingSpec{EncodingType::Dictionary});
          }
        });
      }

      for (auto& thread : threads) thread.join();

      std::cout << " - done (" << timer.lap_formatted() << ")" << std::endl;
    }

    // Print::print(new_table);

    {
      std::cout << "Generating statistics" << std::flush;
      Timer timer;
      drop_table(table_name);
      add_table(table_name, new_table);
      std::cout << " - done (" << timer.lap_formatted() << ")" << std::endl;
    }
  }
}

}  // namespace opossum
