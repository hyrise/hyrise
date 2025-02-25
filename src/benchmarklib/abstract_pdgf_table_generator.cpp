#include "abstract_pdgf_table_generator.hpp"

#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_table_generator.hpp"
#include "benchmark_config.hpp"
#include "hyrise.hpp"
#include "import_export/data_generation/pdgf_process.hpp"
#include "import_export/data_generation/shared_memory_reader.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "types.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace hyrise {
AbstractPDGFTableGenerator::AbstractPDGFTableGenerator(float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config,
                                                       std::vector<std::pair<BenchmarkItemID, std::string>> queries_to_run)
    : AbstractTableGenerator(benchmark_config),
      _scale_factor(scale_factor),
      _queries_to_run(std::move(queries_to_run)),
      _columns_to_generate(std::make_shared<std::set<std::string>>()) {}

#define SHARED_MEMORY_NAME "/PDGF_SHARED_MEMORY"
#define DATA_READY_SEM "/PDGF_DATA_READY_SEM"
#define BUFFER_FREE_SEM "/PDGF_BUFFER_FREE_SEM"

std::unordered_map<std::string, BenchmarkTableInfo> AbstractPDGFTableGenerator::generate() {
  std::unordered_map<std::string, BenchmarkTableInfo> benchmark_tables;
  if (_benchmark_config->separate_benchmark_cycle_per_query) {
    for (const auto& query: _queries_to_run) {
      std::cerr << "Generating ONLY data for query with ID " << query.first << " (note: off-by-one for TPC-H, etc.)\n";
      benchmark_tables = _generate(std::optional{query.second});
    }
  } else {
    benchmark_tables = _generate(std::nullopt);
  }
  return benchmark_tables;
}

std::unordered_map<std::string, BenchmarkTableInfo> AbstractPDGFTableGenerator::_generate(std::optional<std::string> single_query_string) {
  const auto cache_directory = _benchmark_config->binary_tables_cache_directory + "/" + _benchmark_name_short()+ "/sf-" + std::to_string(_scale_factor) + "-partial";  // NOLINT
  std::unordered_map<std::string, BenchmarkTableInfo> table_info_by_name;
  if (_benchmark_config->cache_binary_tables && std::filesystem::is_directory(cache_directory)) {
    table_info_by_name = _load_binary_tables_from_path(cache_directory);
    for (const auto& [table_name, table]: table_info_by_name) {
      Hyrise::get().storage_manager.add_table(table_name, table.table);
    }
  }

  std::cerr << "Setting up shared memory (" << _pdgf_buffer_columns() << " buffer columns).\n";
  const auto reader = create_shared_memory_reader(
    _benchmark_config, _pdgf_buffer_columns(), SHARED_MEMORY_NAME, DATA_READY_SEM, BUFFER_FREE_SEM);
  auto timer = Timer{};

  /**
   * Read schema. Note that the SharedMemoryReader MUST be created first as it will create the shared resources PDGF will
   * bind to.
   */
  if (Hyrise::get().storage_manager.tables().empty()) {
    std::cerr << "Receiving table schemas from PDGF!\n";
    auto pdgf_schema = PdgfProcess::for_schema_generation(
      _pdgf_schema_config_file(), _pdgf_schema_generation_file(), PDGF_DIRECTORY_ROOT, _benchmark_config,
      _pdgf_buffer_columns(), _scale_factor);
    pdgf_schema.run();
    while (reader->has_next_table()) {
      auto schema_builder = reader->read_next_schema();
      // Directly add the (empty) table to the storage manager here.
      // This table will be replaced later once we have received data, but we will need the tables to be present in order for the optimizer to
      // be able to tell us which columns we need to generate for our _queries_to_run
      auto table = schema_builder->build_table();
      Hyrise::get().storage_manager.add_table(schema_builder->table_name(), table);
      // Already insert the tables here because not all of them will be replaced when data is generated later
      table_info_by_name[schema_builder->table_name()].table = table;
    }
    std::cerr << "Awaiting PDGF teardown\n";
    pdgf_schema.await_teardown();
    std::cout << "- Hyrise PDGF: Loading schema done (" << format_duration(timer.lap()) << ")\n" << std::flush;
  } else {
    std::cerr << "Not loading schema from PDGF as we already have loaded tables!\n";
    for (auto& table : Hyrise::get().storage_manager.tables()) {
      table_info_by_name[table.first].table = table.second;
    }
  }

  /**
   * Reset shared memory buffer. This is important because we will proceed to launch PDGF a second time.
   */
  reader->reset();

  /**
   * Collect columns to generate
   */
  if (_benchmark_config->columns_to_generate == ColumnsToGenerate::All) {
    _collect_all_missing_columns();
  } else {
    if (single_query_string) {
      _collect_columns(single_query_string.value());
    } else {
      for (const auto &query_string: _queries_to_run | std::views::values) {
        _collect_columns(query_string);
      }
    }
  }

  /**
   * Generate tables
   */
  if (!_columns_to_generate->empty()) {
    std::cerr << "Generating tables with PDGF\n";
    std::cerr << "Generating columns ";
    for (const auto& column : *_columns_to_generate) {
      std::cerr << column << " ";
    }
    std::cerr << "\n";
    auto pdgf_data = PdgfProcess::for_data_generation(
      _pdgf_schema_config_file(), _pdgf_schema_generation_file(), PDGF_DIRECTORY_ROOT,
      _benchmark_config, _pdgf_buffer_columns(), _scale_factor);
    pdgf_data.set_column_filter(_columns_to_generate);
    pdgf_data.run();
    auto table_builders = std::vector<std::shared_ptr<BasePDGFTableBuilder>>{};
    while (reader->has_next_table()) {
      table_builders.emplace_back(reader->read_next_table());
    }
    std::cerr << "Awaiting PDGF teardown\n";
    pdgf_data.await_teardown();
    std::cout << "- Hyrise PDGF: Generating tables done (" << format_duration(timer.lap()) << ")\n" << std::flush;

    /**
     * Return completely generated tables
     */
    std::cerr << "Finalizing generated tables\n";
    for (auto& table_builder: table_builders) {
      table_info_by_name[table_builder->table_name()].table = table_builder->build_table();
    }

    // Required in case we are doing multiple single-query generations
    _columns_to_generate->clear();
  } else {
    std::cerr << "Not invoking PDGF, all required columns already present!\n";
  }

  // TODO: should not always export the tables, only when data changed!
  if (_benchmark_config->cache_binary_tables) {
    std::filesystem::create_directories(cache_directory);
    for (auto& [table_name, table_info] : table_info_by_name) {
      table_info.binary_file_path = cache_directory + "/" + table_name + ".bin";  // NOLINT
    }
  }

  return table_info_by_name;
}

void AbstractPDGFTableGenerator::_collect_all_missing_columns() {
  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    for (const auto& column : table->column_definitions()) {
      if (!column.loaded) {
        _columns_to_generate->insert(table_name + ":" + column.name);
      }
    }
  }
}

void AbstractPDGFTableGenerator::_collect_columns(const std::string& sql) {
  auto pipeline_builder = SQLPipelineBuilder{sql};
  auto pipeline = pipeline_builder.create_pipeline();

  auto& lqps = pipeline.get_optimized_logical_plans();
  for (const auto& lqp_root : lqps) {
    visit_lqp(lqp_root, [&](const auto& node) {
      if (node->type != LQPNodeType::StoredTable) {
        return LQPVisitation::VisitInputs;
      }

      auto table_node = std::dynamic_pointer_cast<StoredTableNode>(node);
      auto stored_table = Hyrise::get().storage_manager.get_table(table_node->table_name);

      auto pruned_column_ids_iter = table_node->pruned_column_ids().begin();
      for (auto stored_column_id = ColumnID{0}, output_column_id = ColumnID{0};
           stored_column_id < stored_table->column_count(); ++stored_column_id) {
        if (_benchmark_config->columns_to_generate == ColumnsToGenerate::OnlyUsedColumns) {
          // Only prune columns if we do not want to generate whole tables
          if (pruned_column_ids_iter != table_node->pruned_column_ids().end() && stored_column_id == *pruned_column_ids_iter) {
            ++pruned_column_ids_iter;
            continue;
          }
        }

        // non-pruned column
        auto column_info = stored_table->column_definitions()[stored_column_id];
        if (column_info.loaded) {
          // This column appears to have been loaded already as part of a previous (partial) benchmark run.
          continue;
        }
        auto full_name = table_node->table_name + ":" + column_info.name;
        _columns_to_generate->insert(full_name);
        ++output_column_id;
      }
      return LQPVisitation::DoNotVisitInputs;
    });
  }

  // We do not want the LQP to be still present when running the benchmark, as, at this point,
  // the tables have no data and the optimizer was therefore not able to work at its fullest capacity.
  pipeline.lqp_cache->clear();
}
}  // namespace hyrise
