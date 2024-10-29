#include "tpch_pdgf_table_generator.hpp"

#include <cmath>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_table_generator.hpp"
#include "benchmark_config.hpp"
#include "hyrise.hpp"
#include "import_export/data_generation/pdgf_process.hpp"
#include "import_export/data_generation/shared_memory_reader.hpp"
#include "operators/pqp_utils.hpp"
#include "operators/get_table.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpch/tpch_constants.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

TPCHPDGFTableGenerator::TPCHPDGFTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                                       ChunkOffset chunk_size)
    : TPCHPDGFTableGenerator(scale_factor, clustering_configuration, false, std::make_shared<BenchmarkConfig>(chunk_size), std::vector<std::string>{}) {}

TPCHPDGFTableGenerator::TPCHPDGFTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration, bool only_generate_used_columns,
                                               const std::shared_ptr<BenchmarkConfig>& benchmark_config, std::vector<std::string> queries_to_run)
    : AbstractTableGenerator(benchmark_config),
      _scale_factor(scale_factor),
      _only_generate_used_columns(only_generate_used_columns),
      _queries_to_run(std::move(queries_to_run)),
      _columns_to_generate(std::make_shared<std::set<std::string>>()),
      _clustering_configuration(clustering_configuration) {}

#define SHARED_MEMORY_NAME "/PDGF_SHARED_MEMORY"
#define DATA_READY_SEM "/PDGF_DATA_READY_SEM"
#define BUFFER_FREE_SEM "/PDGF_BUFFER_FREE_SEM"

std::unordered_map<std::string, BenchmarkTableInfo> TPCHPDGFTableGenerator::generate() {
  Assert(!_benchmark_config->cache_binary_tables, "Caching of half-empty tables containing dummy segments is currently not supported");
  Assert(_clustering_configuration == ClusteringConfiguration::None, "We do not support any special clustering configurations, as those require sorting, and sorting on PDGF-generated partial data is not supported.");

  std::cerr << "Setting up shared memory.\n";
  auto reader = SharedMemoryReader<128, 16>(_benchmark_config->chunk_size, SHARED_MEMORY_NAME, DATA_READY_SEM, BUFFER_FREE_SEM);
  std::unordered_map<std::string, BenchmarkTableInfo> table_info_by_name;

  /**
   * Read schema. Note that the SharedMemoryReader MUST be created first as it will create the shared resources PDGF will
   * bind to.
   */
  std::cerr << "Receiving table schemas from PDGF!\n";
  auto pdgf_schema = PdgfProcess::for_schema_generation(PDGF_DIRECTORY_ROOT);
  pdgf_schema.run();
  while (reader.has_next_table()) {
    auto schema_builder = reader.read_next_schema();
    // Directly add the (empty) table to the storage manager here.
    // This table will be replaced later once we have received data, but we will need the tables to be present in order for the optimizer to
    // be able to tell us which columns we need to generate for our _queries_to_run
    auto table = schema_builder->build_table();
    Hyrise::get().storage_manager.add_table(schema_builder->table_name(), table);
    // Already insert the tables here because not all of them will be replaced when data is generated later
    table_info_by_name[schema_builder->table_name()].table = table;
  }
  std::cerr << "Awaiting PDGF teardown\n";
  pdgf_schema.wait();

  /**
   * Reset shared memory buffer. This is important because we will proceed to launch PDGF a second time.
   */
  reader.reset();

  /**
   * Collect columns to generate
   */
  if (_only_generate_used_columns) {
    for (const auto& query: _queries_to_run) {
      _collect_columns(query);
    }
  }

  /**
   * Generate tables
   */
  std::cerr << "Generating tables with PDGF\n";
  auto pdgf_data = PdgfProcess::for_data_generation(PDGF_DIRECTORY_ROOT);
  if (_only_generate_used_columns) {
    pdgf_data.set_column_filter(_columns_to_generate);
  }
  pdgf_data.run();
  auto table_builders = std::vector<std::unique_ptr<PDGFTableBuilder<128, 16>>>{};
  while (reader.has_next_table()) {
    table_builders.emplace_back(reader.read_next_table());
  }
  std::cerr << "Awaiting PDGF teardown\n";
  pdgf_data.wait();

  /**
   * Return completely generated tables
   */
  std::cerr << "Finalizing generated tables\n";
  for (auto& table_builder: table_builders) {
    table_info_by_name[table_builder->table_name()].table = table_builder->build_table();
  }

  // TODO(JEH): what about encoding on-the-fly when chunks are done?

  return table_info_by_name;
}

void TPCHPDGFTableGenerator::_collect_columns(const std::string& sql) {
  auto pipeline_builder = SQLPipelineBuilder{sql};
  auto pipeline = pipeline_builder.create_pipeline();
  auto& pqps = pipeline.get_physical_plans();
    for (const auto& pqp_root : pqps) {
      visit_pqp(pqp_root, [&](const auto& node) {
        if (node->type() != OperatorType::GetTable) {
          return PQPVisitation::VisitInputs;
        }

        auto table_node = std::dynamic_pointer_cast<GetTable>(node);
        auto stored_table = Hyrise::get().storage_manager.get_table(table_node->table_name());

        auto pruned_column_ids_iter = table_node->pruned_column_ids().begin();
        for (auto stored_column_id = ColumnID{0}, output_column_id = ColumnID{0};
             stored_column_id < stored_table->column_count(); ++stored_column_id) {
          if (pruned_column_ids_iter != table_node->pruned_column_ids().end() && stored_column_id == *pruned_column_ids_iter) {
            ++pruned_column_ids_iter;
            continue;
          }

          // non-pruned column
          auto column_info = stored_table->column_definitions()[stored_column_id];
          auto full_name = table_node->table_name() + ":" + column_info.name;
          _columns_to_generate->insert(full_name);
          ++output_column_id;
        }
        return PQPVisitation::DoNotVisitInputs;
      });
    }
}

AbstractTableGenerator::IndexesByTable TPCHPDGFTableGenerator::_indexes_by_table() const {
  return {{"part", {{"p_partkey"}}},
          {"supplier", {{"s_suppkey"}, {"s_nationkey"}}},
          {"partsupp", {{"ps_partkey"}, {"ps_suppkey"}}},
          {"customer", {{"c_custkey"}, {"c_nationkey"}}},
          {"orders", {{"o_orderkey"}, {"o_custkey"}}},
          {"lineitem", {{"l_orderkey"}, {"l_partkey"}}},
          {"nation", {{"n_nationkey"}, {"n_regionkey"}}},
          {"region", {{"r_regionkey"}}}};
}

AbstractTableGenerator::SortOrderByTable TPCHPDGFTableGenerator::_sort_order_by_table() const {
  // We DO NOT SUPPORT ANY EXPLICIT SORTING FOR PDGF-GENERATED TABLES AT THE MOMENT.

  // Even though the generated TPC-H data is implicitly sorted by the primary keys, we do neither set the corresponding
  // flags in the table nor in the chunks. This is done on purpose, as the non-clustered mode is designed to pass as
  // little extra information into Hyrise as possible. In the future, these sort orders might be automatically
  // identified with flags being set automatically.
  return {};
}

void TPCHPDGFTableGenerator::_add_constraints(
    std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const {
  // Set all primary (PK) and foreign keys (FK) as defined in the specification (Reision 3.0.1, 1.4.2. Constraints, p.
  // 18).

  // Get all tables.
  const auto& part_table = table_info_by_name.at("part").table;
  const auto& supplier_table = table_info_by_name.at("supplier").table;
  const auto& partsupp_table = table_info_by_name.at("partsupp").table;
  const auto& customer_table = table_info_by_name.at("customer").table;
  const auto& orders_table = table_info_by_name.at("orders").table;
  const auto& lineitem_table = table_info_by_name.at("lineitem").table;
  const auto& nation_table = table_info_by_name.at("nation").table;
  const auto& region_table = table_info_by_name.at("region").table;

  // Set constraints.

  // part - 1 PK.
  primary_key_constraint(part_table, {"p_partkey"});

  // supplier - 1 PK, 1 FK.
  primary_key_constraint(supplier_table, {"s_suppkey"});
  // The FK to n_nationkey is not listed in the list of FKs in 1.4.2, but in the part table layout in 1.4.1, p. 15.
  foreign_key_constraint(supplier_table, {"s_nationkey"}, nation_table, {"n_nationkey"});

  // partsupp - 1 composite PK, 2 FKs.
  primary_key_constraint(partsupp_table, {"ps_partkey", "ps_suppkey"});
  foreign_key_constraint(partsupp_table, {"ps_partkey"}, part_table, {"p_partkey"});
  foreign_key_constraint(partsupp_table, {"ps_suppkey"}, supplier_table, {"s_suppkey"});

  // customer - 1 PK, 1 FK.
  primary_key_constraint(customer_table, {"c_custkey"});
  foreign_key_constraint(customer_table, {"c_nationkey"}, nation_table, {"n_nationkey"});

  // orders - 1 PK, 1 FK.
  primary_key_constraint(orders_table, {"o_orderkey"});
  foreign_key_constraint(orders_table, {"o_custkey"}, customer_table, {"c_custkey"});

  // lineitem - 1 composite PK, 4 FKs.
  primary_key_constraint(lineitem_table, {"l_orderkey", "l_linenumber"});
  foreign_key_constraint(lineitem_table, {"l_orderkey"}, orders_table, {"o_orderkey"});
  // The specification explicitly allows to set the FKs of l_partkey and s_suppkey as a compound FK to partsupp and
  // directly to part/supplier.
  foreign_key_constraint(lineitem_table, {"l_partkey", "l_suppkey"}, partsupp_table, {"ps_partkey", "ps_suppkey"});
  foreign_key_constraint(lineitem_table, {"l_partkey"}, part_table, {"p_partkey"});
  foreign_key_constraint(lineitem_table, {"l_suppkey"}, supplier_table, {"s_suppkey"});

  // nation - 1 PK, 1 FK.
  primary_key_constraint(nation_table, {"n_nationkey"});
  foreign_key_constraint(nation_table, {"n_regionkey"}, region_table, {"r_regionkey"});

  // region - 1 PK.
  primary_key_constraint(region_table, {"r_regionkey"});
}

}  // namespace hyrise
