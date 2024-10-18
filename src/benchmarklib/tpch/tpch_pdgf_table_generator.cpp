#include "tpch_pdgf_table_generator.hpp"

#include <cmath>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_table_generator.hpp"
#include "benchmark_config.hpp"
#include "import_export/data_generation/pdgf_process.hpp"
#include "import_export/data_generation/shared_memory_reader.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "tpch/tpch_constants.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

TPCHPDGFTableGenerator::TPCHPDGFTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                                       ChunkOffset chunk_size)
    : TPCHPDGFTableGenerator(scale_factor, clustering_configuration, std::make_shared<BenchmarkConfig>(chunk_size)) {}

TPCHPDGFTableGenerator::TPCHPDGFTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                                       const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator(benchmark_config),
      _scale_factor(scale_factor),
      _clustering_configuration(clustering_configuration) {}

#define SHARED_MEMORY_NAME "/PDGF_SHARED_MEMORY"
#define DATA_READY_SEM "/PDGF_DATA_READY_SEM"
#define BUFFER_FREE_SEM "/PDGF_BUFFER_FREE_SEM"

std::unordered_map<std::string, BenchmarkTableInfo> TPCHPDGFTableGenerator::generate() {
  Assert(!_benchmark_config->cache_binary_tables, "Caching of half-empty tables containing dummy segments is currently not supported");
  Assert(_clustering_configuration == ClusteringConfiguration::None, "We do not support any special clustering configurations, as those require sorting, and sorting on PDGF-generated partial data is not supported.");

  /**
   * Launch PDGF. Note that the SharedMemoryReader MUST be created first as it will create the shared resources PDGF will
   * bind to.
   */
  std::cout << "Setting up shared memory and launching PDGF.\n";
  auto reader = SharedMemoryReader<128, 16>(_benchmark_config->chunk_size, SHARED_MEMORY_NAME, DATA_READY_SEM, BUFFER_FREE_SEM);
  auto pdgf = PdgfProcess(PDGF_DIRECTORY_ROOT);
  pdgf.run();

  /**
   * Generate tables
   */
  std::cout << "Generating another table with PDGF\n";
  auto table_builders = std::vector<std::unique_ptr<PDGFTableBuilder<128, 16>>>{};
  while (reader.has_next_table()) {
    table_builders.emplace_back(reader.read_next_table());
  }

  /**
   * Await PDGF teardown
   */
  std::cout << "Awaiting PDGF teardown\n";
  pdgf.wait();

  /**
   * Return
   */
  std::cout << "Finalizing generated tables\n";
  std::unordered_map<std::string, BenchmarkTableInfo> table_info_by_name;
  for (auto& table_builder: table_builders) {
    table_info_by_name[table_builder->table_name()].table = table_builder->build_table();
  }

  // TODO(JEH): what about encoding on-the-fly when chunks are done?

  return table_info_by_name;
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
