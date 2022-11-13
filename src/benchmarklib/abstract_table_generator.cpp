#include "abstract_table_generator.hpp"

#include "benchmark_config.hpp"
#include "benchmark_table_encoder.hpp"
#include "hyrise.hpp"
#include "import_export/binary/binary_parser.hpp"
#include "import_export/binary/binary_writer.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/segment_iterate.hpp"
#include "utils/format_duration.hpp"
#include "utils/list_directory.hpp"
#include "utils/timer.hpp"

namespace hyrise {

void to_json(nlohmann::json& json, const TableGenerationMetrics& metrics) {
  json = {{"generation_duration", metrics.generation_duration.count()},
          {"encoding_duration", metrics.encoding_duration.count()},
          {"binary_caching_duration", metrics.binary_caching_duration.count()},
          {"sort_duration", metrics.sort_duration.count()},
          {"store_duration", metrics.store_duration.count()},
          {"index_duration", metrics.index_duration.count()}};
}

BenchmarkTableInfo::BenchmarkTableInfo(const std::shared_ptr<Table>& init_table) : table(init_table) {}

AbstractTableGenerator::AbstractTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : _benchmark_config(benchmark_config) {}

void AbstractTableGenerator::generate_and_store() {
  Timer timer;

  // Encoding table data and generating table statistics are time consuming processes. To reduce the required execution
  // time, we execute these data preparation steps in a multi-threaded way. We store the current scheduler here in case
  // a single-threaded scheduler is used. After data preparation, we switch back to the initially used scheduler.
  const auto initial_scheduler = Hyrise::get().scheduler();
  Hyrise::get().topology.use_default_topology(_benchmark_config->data_preparation_cores);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  std::cout << "- Loading/Generating tables " << std::endl;
  auto table_info_by_name = generate();
  metrics.generation_duration = timer.lap();
  std::cout << "- Loading/Generating tables done (" << format_duration(metrics.generation_duration) << ")" << std::endl;

  /**
   * Finalizing all chunks of all tables that are still mutable.
   */
  // TODO(any): Finalization might trigger encoding in the future.
  for (auto& [table_name, table_info] : table_info_by_name) {
    auto& table = table_info_by_name[table_name].table;
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);
      if (chunk->is_mutable()) {
        chunk->finalize();
      }
    }
  }

  /**
   * Sort tables if a sort order was defined by the benchmark
   */
  {
    const auto& sort_order_by_table = _sort_order_by_table();
    if (sort_order_by_table.empty()) {
      // If there is no clustering for the benchmark defined, there should not be a single sorted chunk. This check is
      // necessary to avoid loading sorted binary data (created with a clustering configuration) in a run that is
      // supposed to be unclustered.
      for (auto& [table_name, table_info] : table_info_by_name) {
        auto& table = table_info_by_name[table_name].table;
        for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
          const auto chunk = table->get_chunk(chunk_id);
          Assert(chunk->individually_sorted_by().empty(),
                 "Tables are sorted, but no clustering has been requested. "
                 "This might be case when clustered data is loaded from "
                 "mismatching binary exports.");
        }
      }
    } else {
      std::cout << "- Sorting tables" << std::endl;

      auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
      jobs.reserve(sort_order_by_table.size());
      for (const auto& sort_order_pair : sort_order_by_table) {
        // Cannot use structured binding here as it cannot be captured in the lambda:
        // http://www.open-std.org/jtc1/sc22/wg21/docs/cwg_defects.html#2313
        const auto& table_name = sort_order_pair.first;
        const auto& column_name = sort_order_pair.second;

        const auto sort_table = [&]() {
          auto& table = table_info_by_name[table_name].table;
          const auto sort_mode = SortMode::Ascending;  // currently fixed to ascending
          const auto sort_column_id = table->column_id_by_name(column_name);
          const auto chunk_count = table->chunk_count();

          // Check if table is already sorted
          auto is_sorted = true;
          resolve_data_type(table->column_data_type(sort_column_id), [&](auto type) {
            using ColumnDataType = typename decltype(type)::type;

            auto last_value = std::optional<ColumnDataType>{};
            for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
              const auto& segment = table->get_chunk(chunk_id)->get_segment(sort_column_id);
              segment_with_iterators<ColumnDataType>(*segment, [&](auto it, const auto end) {
                while (it != end) {
                  if (it->is_null()) {
                    if (last_value) {
                      // NULLs should come before all values
                      is_sorted = false;
                      break;
                    }

                    ++it;
                    continue;
                  }

                  if (!last_value || it->value() >= *last_value) {
                    last_value = it->value();
                  } else {
                    is_sorted = false;
                    break;
                  }

                  ++it;
                }
              });
            }
          });

          if (is_sorted) {
            auto output = std::stringstream{};
            output << "-  Table '" << table_name << "' is already sorted by '" << column_name << "'\n";
            std::cout << output.str() << std::flush;
            const SortColumnDefinition sort_column{sort_column_id, sort_mode};

            if (_all_chunks_sorted_by(table, sort_column)) {
              return;
            }

            for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
              const auto& chunk = table->get_chunk(chunk_id);
              Assert(chunk->individually_sorted_by().empty(), "Chunk SortColumnDefinitions need to be empty");
              chunk->set_individually_sorted_by(sort_column);
            }

            return;
          }

          // We sort the tables after their creation so that we are independent of the order in which they are filled.
          // For this, we use the sort operator. Because it returns a `const Table`, we need to recreate the table and
          // migrate the sorted chunks to that table.

          Timer per_table_timer;

          auto table_wrapper = std::make_shared<TableWrapper>(table);
          table_wrapper->execute();
          auto sort = std::make_shared<Sort>(
              table_wrapper, std::vector<SortColumnDefinition>{SortColumnDefinition{sort_column_id, sort_mode}},
              _benchmark_config->chunk_size, Sort::ForceMaterialization::Yes);
          sort->execute();
          const auto immutable_sorted_table = sort->get_output();

          Assert(immutable_sorted_table->chunk_count() == table->chunk_count(), "Mismatching chunk_count");

          table = std::make_shared<Table>(immutable_sorted_table->column_definitions(), TableType::Data,
                                          table->target_chunk_size(), UseMvcc::Yes);
          const auto column_count = immutable_sorted_table->column_count();
          for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
            const auto chunk = immutable_sorted_table->get_chunk(chunk_id);
            auto mvcc_data = std::make_shared<MvccData>(chunk->size(), CommitID{0});
            Segments segments{};
            for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
              segments.emplace_back(chunk->get_segment(column_id));
            }
            table->append_chunk(segments, mvcc_data);
            table->get_chunk(chunk_id)->finalize();
            table->get_chunk(chunk_id)->set_individually_sorted_by(SortColumnDefinition(sort_column_id, sort_mode));
          }

          auto output = std::stringstream{};
          output << "-  Sorted '" << table_name << "' by '" << column_name << "' (" << per_table_timer.lap_formatted()
                 << ")\n";
          std::cout << output.str() << std::flush;
        };
        jobs.emplace_back(std::make_shared<JobTask>(sort_table));
      }
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

      metrics.sort_duration = timer.lap();
      std::cout << "- Sorting tables done (" << format_duration(metrics.sort_duration) << ")" << std::endl;
    }
  }

  /**
   * Add constraints if defined by the benchmark
   */
  _add_constraints(table_info_by_name);

  /**
   * Encode the tables
   */
  {
    std::cout << "- Encoding tables (if necessary) and generating pruning statistics" << std::endl;

    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(table_info_by_name.size());
    for (auto& table_info_by_name_pair : table_info_by_name) {
      const auto& table_name = table_info_by_name_pair.first;
      auto& table_info = table_info_by_name_pair.second;

      const auto encode_table = [&]() {
        Timer per_table_timer;
        table_info.re_encoded =
            BenchmarkTableEncoder::encode(table_name, table_info.table, _benchmark_config->encoding_config);
        auto output = std::stringstream{};
        output << "-  Encoding '" + table_name << "' - "
               << (table_info.re_encoded ? "encoding applied" : "no encoding necessary") << " ("
               << per_table_timer.lap_formatted() << ")\n";
        std::cout << output.str() << std::flush;
      };
      jobs.emplace_back(std::make_shared<JobTask>(encode_table));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

    metrics.encoding_duration = timer.lap();
    std::cout << "- Encoding tables and generating pruning statistic done ("
              << format_duration(metrics.encoding_duration) << ")" << std::endl;
  }

  /**
   * Write the Tables into binary files if required
   */
  if (_benchmark_config->cache_binary_tables) {
    for (auto& [table_name, table_info] : table_info_by_name) {
      const auto& table = table_info.table;
      if (table->chunk_count() > 1 && table->get_chunk(ChunkID{0})->size() != _benchmark_config->chunk_size) {
        Fail("Table '" + table_name + "' was loaded from binary, but has a mismatching chunk size of " +
             std::to_string(table->get_chunk(ChunkID{0})->size()) +
             ". Delete cached files or use '--dont_cache_binary_tables'.");
      }
    }

    std::cout << "- Writing tables into binary files if necessary" << std::endl;

    for (auto& [table_name, table_info] : table_info_by_name) {
      if (table_info.loaded_from_binary && !table_info.re_encoded && !table_info.binary_file_out_of_date) {
        continue;
      }

      auto binary_file_path = std::filesystem::path{};
      if (table_info.binary_file_path) {
        binary_file_path = *table_info.binary_file_path;
      } else {
        binary_file_path = *table_info.text_file_path;
        binary_file_path.replace_extension(".bin");
      }

      std::cout << "-  Writing '" << table_name << "' into binary file " << binary_file_path << " " << std::flush;
      Timer per_table_timer;
      BinaryWriter::write(*table_info.table, binary_file_path);
      std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;
    }
    metrics.binary_caching_duration = timer.lap();
    std::cout << "- Writing tables into binary files done (" << format_duration(metrics.binary_caching_duration) << ")"
              << std::endl;
  }

  /**
   * Add the Tables to the StorageManager
   */
  {
    std::cout << "- Adding tables to StorageManager and generating table statistics" << std::endl;
    auto& storage_manager = Hyrise::get().storage_manager;
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(table_info_by_name.size());
    for (auto& table_info_by_name_pair : table_info_by_name) {
      const auto& table_name = table_info_by_name_pair.first;
      auto& table_info = table_info_by_name_pair.second;

      const auto add_table = [&]() {
        Timer per_table_timer;
        if (storage_manager.has_table(table_name)) {
          storage_manager.drop_table(table_name);
        }
        storage_manager.add_table(table_name, table_info.table);
        const auto output =
            std::string{"-  Added '"} + table_name + "' " + "(" + per_table_timer.lap_formatted() + ")\n";
        std::cout << output << std::flush;
      };
      jobs.emplace_back(std::make_shared<JobTask>(add_table));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

    metrics.store_duration = timer.lap();

    std::cout << "- Adding tables to StorageManager and generating table statistics done ("
              << format_duration(metrics.store_duration) << ")" << std::endl;
  }

  /**
   * Create indexes if requested by the user
   */
  if (_benchmark_config->indexes) {
    std::cout << "- Creating indexes" << std::endl;
    const auto& indexes_by_table = _indexes_by_table();
    if (indexes_by_table.empty()) {
      std::cout << "-  No indexes defined by benchmark" << std::endl;
    }
    for (const auto& [table_name, indexes] : indexes_by_table) {
      const auto& table = table_info_by_name[table_name].table;

      for (const auto& index_columns : indexes) {
        auto column_ids = std::vector<ColumnID>{};

        for (const auto& index_column : index_columns) {
          column_ids.emplace_back(table->column_id_by_name(index_column));
        }

        std::cout << "-  Creating index on " << table_name << " [ ";
        for (const auto& index_column : index_columns) {
          std::cout << index_column << " ";
        }
        std::cout << "] " << std::flush;
        Timer per_index_timer;

        if (column_ids.size() == 1) {
          table->create_index<GroupKeyIndex>(column_ids);
        } else {
          table->create_index<CompositeGroupKeyIndex>(column_ids);
        }

        std::cout << "(" << per_index_timer.lap_formatted() << ")" << std::endl;
      }
    }
    metrics.index_duration = timer.lap();
    std::cout << "- Creating indexes done (" << format_duration(metrics.index_duration) << ")" << std::endl;
  } else {
    std::cout << "- No indexes created as --indexes was not specified or set to false" << std::endl;
  }

  // Set scheduler back to previously used scheduler.
  Hyrise::get().topology.use_default_topology(_benchmark_config->cores);
  Hyrise::get().set_scheduler(initial_scheduler);
}

std::shared_ptr<BenchmarkConfig> AbstractTableGenerator::create_benchmark_config_with_chunk_size(
    ChunkOffset chunk_size) {
  auto config = BenchmarkConfig::get_default_config();
  config.chunk_size = chunk_size;
  return std::make_shared<BenchmarkConfig>(config);
}

AbstractTableGenerator::IndexesByTable AbstractTableGenerator::_indexes_by_table() const {
  return {};
}

AbstractTableGenerator::SortOrderByTable AbstractTableGenerator::_sort_order_by_table() const {
  return {};
}

void AbstractTableGenerator::_add_constraints(
    std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const {}

bool AbstractTableGenerator::_all_chunks_sorted_by(const std::shared_ptr<Table>& table,
                                                   const SortColumnDefinition& sort_column) {
  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto& sorted_columns = table->get_chunk(chunk_id)->individually_sorted_by();
    if (sorted_columns.empty()) {
      return false;
    }
    bool chunk_sorted = false;
    for (const auto& sorted_column : sorted_columns) {
      if (sorted_column.column == sort_column.column) {
        Assert(sorted_column.sort_mode == sort_column.sort_mode, "Column is already sorted by another SortMode");
        chunk_sorted = true;
      }
    }
    if (!chunk_sorted) {
      return false;
    }
  }
  return true;
}

std::unordered_map<std::string, BenchmarkTableInfo> AbstractTableGenerator::_load_binary_tables_from_path(
    const std::string& cache_directory) {
  std::unordered_map<std::string, BenchmarkTableInfo> table_info_by_name;

  for (const auto& table_file : list_directory(cache_directory)) {
    const auto table_name = table_file.stem();
    std::cout << "-  Loading table '" << table_name.string() << "' from cached binary " << table_file.relative_path();

    Timer timer;
    BenchmarkTableInfo table_info;
    table_info.table = BinaryParser::parse(table_file);
    table_info.loaded_from_binary = true;
    table_info.binary_file_path = table_file;
    table_info_by_name[table_name] = table_info;

    std::cout << " (" << timer.lap_formatted() << ")" << std::endl;
  }

  return table_info_by_name;
}

}  // namespace hyrise
