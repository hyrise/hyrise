#include "tpch_pdgf_table_generator.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_table_generator.hpp"
#include "benchmark_config.hpp"
#include "import_export/data_generation/pdgf_process.hpp"
#include "import_export/data_generation/shared_memory_reader.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "table_builder.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch_constants.hpp"
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
  Assert(_scale_factor < 1.0f || std::round(_scale_factor) == _scale_factor,
         "Due to tpch_dbgen limitations, only scale factors less than one can have a fractional part.");

  const auto cache_directory = std::string{"tpch_cached_tables/sf-"} + std::to_string(_scale_factor);  // NOLINT
  if (_benchmark_config->cache_binary_tables && std::filesystem::is_directory(cache_directory)) {
    return _load_binary_tables_from_path(cache_directory);
  }

  /**
   * Launch PDGF. Note that the SharedMemoryReader MUST be created first as it will create the shared resources PDGF will
   * bind to.
   */
  auto reader = SharedMemoryReader<128, 16>(_benchmark_config->chunk_size, SHARED_MEMORY_NAME, DATA_READY_SEM, BUFFER_FREE_SEM);
  auto pdgf = PdgfProcess(PDGF_DIRECTORY_ROOT);
  pdgf.run();

  /**
   * Generate tables
   */
  while (reader.has_next_table()) {
    reader.read_next_table();
  }


  /**
   * Await PDGF teardown
   */
  pdgf.wait();

  /**
   * Return
   */
  std::unordered_map<std::string, BenchmarkTableInfo> table_info_by_name;
//  table_info_by_name["customer"].table = customer_table;
//  table_info_by_name["orders"].table = orders_table;
//  table_info_by_name["lineitem"].table = lineitem_table;
//  table_info_by_name["part"].table = part_table;
//  table_info_by_name["partsupp"].table = partsupp_table;
//  table_info_by_name["supplier"].table = supplier_table;
//  table_info_by_name["nation"].table = nation_table;
//  table_info_by_name["region"].table = region_table;

  if (_benchmark_config->cache_binary_tables) {
    std::filesystem::create_directories(cache_directory);
    for (auto& [table_name, table_info] : table_info_by_name) {
      table_info.binary_file_path = cache_directory + "/" + table_name + ".bin";  // NOLINT
    }
  }

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
  if (_clustering_configuration == ClusteringConfiguration::Pruning) {
    // This clustering improves the pruning of chunks for the two largest tables in TPC-H, lineitem and orders. Both
    // tables are frequently filtered by the sorted columns, which improves the pruning rate significantly.
    // Allowed as per TPC-H Specification, paragraph 1.5.2.
    return {{"lineitem", "l_shipdate"}, {"orders", "o_orderdate"}};
  }

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
