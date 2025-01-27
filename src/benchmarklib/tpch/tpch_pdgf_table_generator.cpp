#include "tpch_pdgf_table_generator.hpp"

#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_pdgf_table_generator.hpp"
#include "abstract_table_generator.hpp"
#include "benchmark_config.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "tpch/tpch_constants.hpp"
#include "types.hpp"

namespace hyrise {

TPCHPDGFTableGenerator::TPCHPDGFTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                                       ChunkOffset chunk_size)
    : TPCHPDGFTableGenerator(scale_factor, clustering_configuration, std::make_shared<BenchmarkConfig>(chunk_size), std::vector<std::pair<BenchmarkItemID, std::string>>{}) {}

TPCHPDGFTableGenerator::TPCHPDGFTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                                               const std::shared_ptr<BenchmarkConfig>& benchmark_config, std::vector<std::pair<BenchmarkItemID, std::string>> queries_to_run)
    : AbstractPDGFTableGenerator(scale_factor, benchmark_config, std::move(queries_to_run)),
    _clustering_configuration(clustering_configuration) {}

std::unordered_map<std::string, BenchmarkTableInfo> TPCHPDGFTableGenerator::generate() {
    Assert(_clustering_configuration == ClusteringConfiguration::None, "We do not support any special clustering configurations, as those require sorting, and sorting on PDGF-generated partial data is not supported.");
    return AbstractPDGFTableGenerator::generate();
}

std::string TPCHPDGFTableGenerator::_pdgf_schema_config_file() const {
  return "pdgf-core_config_tpc-h-schema.xml";
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

  // If we already added the constraints previously (because we are executing the benchmark in multiple separate steps),
  // we don't need to do so again.
  if (!part_table->soft_key_constraints().empty()) {
    std::cout << "[WARNING] Skipped adding constraints to TPC-H tables because we already have done so, apparently!\n";
    return;
  }

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
