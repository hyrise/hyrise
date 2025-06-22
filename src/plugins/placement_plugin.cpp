#include "placement_plugin.hpp"

#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>

#include "../benchmarklib/abstract_benchmark_item_runner.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_writer.hpp"
#include "operators/print.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"
#include "utils/string_utils.hpp"

#include "magic_enum.hpp"

namespace hyrise {

inline void create_table_access_snapshot(const char* table_name){
  std::cout << "- Create access statistics snapshot table...\n";
  auto ss = std::stringstream{};
  ss << "CREATE TABLE " << table_name << R"( AS
    SELECT
      table_name, column_name, SUM(point_accesses) AS point, SUM(sequential_accesses) AS seq, SUM(monotonic_accesses)
      AS mon, SUM(random_accesses) AS rnd, SUM(dictionary_accesses) AS dict, SUM(size_in_bytes) AS bytes
    FROM
      meta_segments_accurate
    GROUP BY table_name, column_name)";
  SQLPipelineBuilder{ss.str()}.create_pipeline().get_result_table();
}

inline void create_table_access_delta(const char* table_name, const char* table_name_before, const char* table_name_after){
  std::cout << "- Create access statistics delta table...\n";
  auto ss = std::stringstream{};
  ss << "CREATE TABLE " << table_name << " AS ";
  ss << R"(SELECT
            i.table_name AS table_name, i.column_name AS column_name, i.point AS point, i.seq AS seq, i.mon AS mon, i.rnd AS rnd, i.dict AS dict,
            (i.point + i.seq + i.mon + i.rnd + i.dict) AS total_accesses, (CAST(i.bytes AS FLOAT) / (1000000000)) AS size_GB
          FROM (
            SELECT
              "t2".table_name, "t2".column_name,
              ("t2".point - "t1".point) AS point,
              ("t2".seq - "t1".seq) AS seq,
              ("t2".mon - "t1".mon) AS mon,
              ("t2".rnd - "t1".rnd) AS rnd,
              ("t2".dict - "t1".dict) AS dict,
              "t2".bytes AS bytes
            FROM )";
  ss << table_name_after << R"( AS "t2" JOIN )" << table_name_before << R"( AS "t1" ON t2.column_name = t1.column_name)";
  ss << ") AS i ORDER BY total_accesses DESC";
  SQLPipelineBuilder{ss.str()}.create_pipeline().get_result_table();
}

inline void print_table_sizes() {
  auto ss = std::stringstream{};
  ss << R"(
    SELECT
      i.table_name, (CAST(i.bytes AS FLOAT) / (1000000000)) AS size_GB
    FROM (
      SELECT table_name, SUM(size_in_bytes) AS bytes
      FROM meta_segments_accurate
      GROUP BY table_name
    ) AS i
    WHERE i.table_name IN ('region', 'customer', 'nation', 'supplier', 'lineitem', 'part', 'orders', 'partsupp')
  )";
  Print::print(ss.str());
}

inline void print_total_tables_size() {
  auto ss = std::stringstream{};
  ss << R"(
    SELECT
      (CAST(SUM(i.bytes) AS FLOAT) / (1000000000)) AS size_GB
    FROM (
      SELECT table_name, SUM(size_in_bytes) AS bytes
      FROM meta_segments_accurate
      WHERE table_name IN ('region', 'customer', 'nation', 'supplier', 'lineitem', 'part', 'orders', 'partsupp')
      GROUP BY table_name
    ) AS i
  )";
  Print::print(ss.str());
}

inline void print_table(const std::string_view table_name){
  auto ss = std::stringstream{};
  ss << "SELECT * FROM " << table_name;
  Print::print(ss.str());
}

std::string PlacementPlugin::description() const {
  return "PlacementPlugin";
}

void PlacementPlugin::start() {
  // TODO(MW)
}

void PlacementPlugin::stop() {
  // TODO(MW) stop
}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> PlacementPlugin::provided_user_executable_functions() {
  return {};
}

void PlacementPlugin::run_page_placement(AbstractBenchmarkItemRunner& benchmark_item_runner) {
  std::cout << "Run page placement" << std::endl;
  auto& resources = Hyrise::get().memory_resources;
  auto resource = resources["cxl"].get();
  auto linear_resource = dynamic_cast<LinearNumaMemoryResource*>(resource);
  Assert(resource, "Cast to LinearNumaMemoryResource* failed");
  auto& storage_manager = Hyrise::get().storage_manager;

  const auto& options = Hyrise::get().placement_options;
  const auto nodes_str = values_to_string(linear_resource->node_ids());
  std::cout << "- Migrating table data with " << magic_enum::enum_name(options.type) << " accross nodes " << nodes_str << std::endl;
  auto per_table_timer = Timer{};
  for (auto& [table_name, table] : storage_manager.tables()) {
    table->migrate(linear_resource);
    std::cout << "-  Migrated '" << table_name << "' (" << per_table_timer.lap_formatted() << ")" << std::endl;
  }
}

void PlacementPlugin::run_column_placement(AbstractBenchmarkItemRunner& benchmark_item_runner) {
  std::cout << "Run column placement\n" << std::flush;
  const auto& options = Hyrise::get().placement_options;
  Assert(!options.lmem_node_ids.empty(), "lmem node ids should not be empty for column placements.");
  if (!options.lmem_node_ids.empty()) {
      auto memory_resource = std::make_unique<LinearNumaMemoryResource>(options.scale_factor * SIZE_GIB * 1.0, options.lmem_node_ids);
      Hyrise::get().memory_resources.emplace("local", std::move(memory_resource));
  }
  constexpr auto table_name_before = "segment_accesses_before_workload";
  constexpr auto table_name_after = "segment_accesses_after_workload";
  constexpr auto table_name_delta = "segment_accesses_delta_workload";

  create_table_access_snapshot(table_name_before);
  for (const auto item_id : benchmark_item_runner.items()) {
    benchmark_item_runner.execute_item(item_id);
  }
  create_table_access_snapshot(table_name_after);
  create_table_access_delta(table_name_delta, table_name_before, table_name_after);
  ////////// COLUMN PLACEMENT
  auto ss = std::stringstream{};
  ss << "SELECT table_name, column_name, total_accesses, size_GB FROM " << table_name_delta;
  const auto [pipeline_status, access_table] = SQLPipelineBuilder{ss.str()}.create_pipeline().get_result_table();
  auto csv_path = options.json_path;
  constexpr auto dot_json_length = 5u; // length of ".json"
  csv_path = csv_path.substr(0, csv_path.size() - dot_json_length) + "_accesses.csv";
  CsvWriter::write(*access_table, csv_path);
//  Print::print(access_table);
//  print_table_sizes();
//  print_total_tables_size();

  // Aggregate column ids to migrate for each table
  auto column_ids_to_migrate = std::unordered_map<pmr_string, std::vector<ColumnID>>{};
  auto column_names_to_migrate = std::unordered_map<pmr_string, std::vector<pmr_string>>{};
  auto& stm = Hyrise::get().storage_manager;
  auto row_idx = 0u;
  for (auto& row : access_table->get_rows()) {
    const auto table_name = boost::get<pmr_string>(row[0]);
    const auto column_name = boost::get<pmr_string>(row[1]);
    if (row_idx < options.num_most_frequently_columns_local) {
      ++row_idx;
      continue;
    }
    const auto column_id = stm.get_table(std::string(table_name))->column_id_by_name(std::string(column_name));
    column_ids_to_migrate[table_name].push_back(column_id);
    column_names_to_migrate[table_name].push_back(column_name);
    ++row_idx;
  }

  // Actual migration
  auto& resources = Hyrise::get().memory_resources;
  Assert(resources.contains("cxl"), "resource for 'cxl' does not exitst");
  Assert(resources.contains("local"), "resource for 'local' does not exitst");

  auto cxl_resource = resources["cxl"].get();
  auto cxl_linear_resource = dynamic_cast<LinearNumaMemoryResource*>(cxl_resource);
  Assert(cxl_linear_resource, "Cast to LinearNumaMemoryResource* failed");

  auto local_resource = resources["local"].get();
  auto local_linear_resource = dynamic_cast<LinearNumaMemoryResource*>(local_resource);
  Assert(local_linear_resource, "Cast to LinearNumaMemoryResource* failed");

  for (auto& [table_name,column_ids] : column_ids_to_migrate) {
    auto table = stm.get_table(std::string(table_name));
    std::cout << "Migrating columns of table " << table_name << ": ";
    std::sort(column_ids.begin(), column_ids.end());
    for (auto& col : column_names_to_migrate[table_name]) {
      std::cout << col << ", ";
    }
    std::cout << "\n" << std::flush;
    table->migrate_columns(column_ids, cxl_linear_resource, local_linear_resource);
  }
}

std::optional<PreBenchmarkHook> PlacementPlugin::pre_benchmark_hook() {
  return [&](auto& benchmark_item_runner) {
    const auto& options = Hyrise::get().placement_options;
    std::cout << "Options [" << options << "]" << std::endl;
    if (!options.rmem_node_ids.empty()) {
      auto memory_resource = std::make_unique<LinearNumaMemoryResource>(options.scale_factor * SIZE_GIB * 1.0, options.rmem_node_ids, options.rmem_weights);
      Hyrise::get().memory_resources.emplace("cxl", std::move(memory_resource));
    }

    auto& resources = Hyrise::get().memory_resources;
    if (!resources.contains("cxl")) {
      std::cout << "- Not migrating any table data as no memory resource was configured." << std::endl;
      return;
    }

    switch (options.type) {
      case PlacementType::AccessFrequency:
          run_column_placement(benchmark_item_runner);
          break;
      case PlacementType::PagesRoundRobinInterleaved:
      case PlacementType::PagesWeightedInterleaved:
          run_page_placement(benchmark_item_runner);
          break;
      case PlacementType::None:
          std::cout << "- Not migrating any table data as placement type is None." << std::endl;
          return;
    }
  };
}

std::optional<PostBenchmarkHook> PlacementPlugin::post_benchmark_hook() {
  return [&](auto& report) {
    // Nothing
  };
}

EXPORT_PLUGIN(PlacementPlugin);

}  // namespace hyrise
