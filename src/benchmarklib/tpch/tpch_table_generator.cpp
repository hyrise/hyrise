#include "tpch_table_generator.hpp"

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

extern "C" {
#include "dss.h"
#include "dsstypes.h"
#include "tpch_dbgen.h"
}

#include "abstract_table_generator.hpp"
#include "benchmark_config.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "table_builder.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch_constants.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

extern const char** asc_date;  // NOLINT
extern seed_t seed[];          // NOLINT

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

// clang-format off
const auto customer_column_types = boost::hana::tuple      <int32_t,    pmr_string,  pmr_string,  int32_t,       pmr_string,  float,       pmr_string,     pmr_string>();  // NOLINT
const auto customer_column_names = boost::hana::make_tuple("c_custkey", "c_name",    "c_address", "c_nationkey", "c_phone",   "c_acctbal", "c_mktsegment", "c_comment"); // NOLINT

const auto order_column_types = boost::hana::tuple      <int32_t,     int32_t,     pmr_string,      float,          pmr_string,    pmr_string,        pmr_string,  int32_t,          pmr_string>();  // NOLINT
const auto order_column_names = boost::hana::make_tuple("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk",   "o_shippriority", "o_comment");  // NOLINT

const auto lineitem_column_types = boost::hana::tuple      <int32_t,     int32_t,     int32_t,     int32_t,        float,        float,             float,        float,   pmr_string,     pmr_string,     pmr_string,   pmr_string,     pmr_string,      pmr_string,       pmr_string,   pmr_string>();  // NOLINT
const auto lineitem_column_names = boost::hana::make_tuple("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment");  // NOLINT

const auto part_column_types = boost::hana::tuple      <int32_t,    pmr_string,  pmr_string,  pmr_string,  pmr_string,  int32_t,  pmr_string,    float,        pmr_string>();  // NOLINT
const auto part_column_names = boost::hana::make_tuple("p_partkey", "p_name",    "p_mfgr",    "p_brand",   "p_type",    "p_size", "p_container", "p_retailprice", "p_comment");  // NOLINT

const auto partsupp_column_types = boost::hana::tuple<     int32_t,      int32_t,      int32_t,       float,           pmr_string>();  // NOLINT
const auto partsupp_column_names = boost::hana::make_tuple("ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment");  // NOLINT

const auto supplier_column_types = boost::hana::tuple<     int32_t,     pmr_string,  pmr_string,  int32_t,       pmr_string,  float,       pmr_string>();  // NOLINT
const auto supplier_column_names = boost::hana::make_tuple("s_suppkey", "s_name",    "s_address", "s_nationkey", "s_phone",   "s_acctbal", "s_comment");  // NOLINT

const auto nation_column_types = boost::hana::tuple<     int32_t,       pmr_string,  int32_t,       pmr_string>();  // NOLINT
const auto nation_column_names = boost::hana::make_tuple("n_nationkey", "n_name",    "n_regionkey", "n_comment");  // NOLINT

const auto region_column_types = boost::hana::tuple<     int32_t,       pmr_string,  pmr_string>();  // NOLINT
const auto region_column_names = boost::hana::make_tuple("r_regionkey", "r_name",    "r_comment");  // NOLINT
// clang-format on

const std::unordered_map<TPCHTable, std::underlying_type_t<TPCHTable>> tpch_table_to_dbgen_id = {
    {TPCHTable::Part, PART},    {TPCHTable::PartSupp, PSUPP}, {TPCHTable::Supplier, SUPP}, {TPCHTable::Customer, CUST},
    {TPCHTable::Orders, ORDER}, {TPCHTable::LineItem, LINE},  {TPCHTable::Nation, NATION}, {TPCHTable::Region, REGION}};

template <typename DSSType, typename MKRetType, typename... Args>
DSSType call_dbgen_mk(size_t idx, MKRetType (*mk_fn)(DSS_HUGE, DSSType* val, Args...), TPCHTable table, Args... args) {
  /**
   * Preserve calling scheme (row_start(); mk...(); row_stop(); as in dbgen's gen_tbl())
   */

  const auto dbgen_table_id = tpch_table_to_dbgen_id.at(table);

  row_start(dbgen_table_id);

  DSSType value{};
  mk_fn(idx, &value, std::forward<Args>(args)...);

  row_stop(dbgen_table_id);

  return value;
}

float convert_money(DSS_HUGE cents) {
  const auto dollars = cents / 100;
  cents %= 100;
  return static_cast<float>(dollars) + (static_cast<float>(cents)) / 100.0f;
}

/**
 * Call this after using dbgen to avoid memory leaks.
 */
void dbgen_cleanup() {
  for (auto* distribution : {&nations,     &regions,        &o_priority_set, &l_instruct_set,
                             &l_smode_set, &l_category_set, &l_rflag_set,    &c_mseg_set,
                             &colors,      &p_types_set,    &p_cntr_set,     &articles,
                             &nouns,       &adjectives,     &adverbs,        &prepositions,
                             &verbs,       &terminators,    &auxillaries,    &np,
                             &vp,          &grammar}) {
    // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,hicpp-no-malloc,cppcoreguidelines-owning-memory)
    std::free(distribution->permute);
    distribution->permute = nullptr;
  }

  if (asc_date) {
    for (auto idx = size_t{0}; idx < TOTDATE; ++idx) {
      // NOLINTBEGIN(cppcoreguidelines-no-malloc,hicpp-no-malloc)
      // NOLINTBEGIN(cppcoreguidelines-owning-memory,cppcoreguidelines-pro-type-const-cast)
      std::free(const_cast<char*>(asc_date[idx]));
      // NOLINTEND(cppcoreguidelines-owning-memory,cppcoreguidelines-pro-type-const-cast)
      // NOLINTEND(cppcoreguidelines-no-malloc,hicpp-no-malloc)
    }
    std::free(asc_date);  // NOLINT(cppcoreguidelines-no-malloc,hicpp-no-malloc,cppcoreguidelines-owning-memory)
  }
  asc_date = nullptr;
}

}  // namespace

namespace hyrise {

const std::unordered_map<TPCHTable, std::string> tpch_table_names = {
    {TPCHTable::Part, "part"},         {TPCHTable::PartSupp, "partsupp"}, {TPCHTable::Supplier, "supplier"},
    {TPCHTable::Customer, "customer"}, {TPCHTable::Orders, "orders"},     {TPCHTable::LineItem, "lineitem"},
    {TPCHTable::Nation, "nation"},     {TPCHTable::Region, "region"}};

TPCHTableGenerator::TPCHTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                                       ChunkOffset chunk_size)
    : TPCHTableGenerator(scale_factor, clustering_configuration, std::make_shared<BenchmarkConfig>(chunk_size)) {}

TPCHTableGenerator::TPCHTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                                       const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator(benchmark_config),
      _scale_factor(scale_factor),
      _clustering_configuration(clustering_configuration) {}

std::unordered_map<std::string, BenchmarkTableInfo> TPCHTableGenerator::generate() {
  Assert(_scale_factor < 1.0f || std::round(_scale_factor) == _scale_factor,
         "Due to tpch_dbgen limitations, only scale factors less than one can have a fractional part.");

  const auto cache_directory = std::string{"tpch_cached_tables/sf-"} + std::to_string(_scale_factor);  // NOLINT
  if (_benchmark_config->cache_binary_tables && std::filesystem::is_directory(cache_directory)) {
    return _load_binary_tables_from_path(cache_directory);
  }

  // Init tpch_dbgen - it is important this is done before any data structures from tpch_dbgen are read.
  dbgen_reset_seeds();
  dbgen_init_scale_factor(_scale_factor);

  const auto customer_count = static_cast<ChunkOffset>(tdefs[CUST].base * scale);
  const auto order_count = static_cast<ChunkOffset>(tdefs[ORDER].base * scale);
  const auto part_count = static_cast<ChunkOffset>(tdefs[PART].base * scale);
  const auto supplier_count = static_cast<ChunkOffset>(tdefs[SUPP].base * scale);
  const auto nation_count = static_cast<ChunkOffset>(tdefs[NATION].base);
  const auto region_count = static_cast<ChunkOffset>(tdefs[REGION].base);

  // The `* 4` part is defined in the TPC-H specification.
  auto customer_builder =
      TableBuilder{_benchmark_config->chunk_size, customer_column_types, customer_column_names, customer_count};
  auto order_builder = TableBuilder{_benchmark_config->chunk_size, order_column_types, order_column_names, order_count};
  auto lineitem_builder = TableBuilder{_benchmark_config->chunk_size, lineitem_column_types, lineitem_column_names,
                                       ChunkOffset{order_count * 4}};
  auto part_builder = TableBuilder{_benchmark_config->chunk_size, part_column_types, part_column_names, part_count};
  auto partsupp_builder = TableBuilder{_benchmark_config->chunk_size, partsupp_column_types, partsupp_column_names,
                                       ChunkOffset{part_count * 4}};
  auto supplier_builder =
      TableBuilder{_benchmark_config->chunk_size, supplier_column_types, supplier_column_names, supplier_count};
  auto nation_builder =
      TableBuilder{_benchmark_config->chunk_size, nation_column_types, nation_column_names, nation_count};
  auto region_builder =
      TableBuilder{_benchmark_config->chunk_size, region_column_types, region_column_names, region_count};

  /**
   * CUSTOMER
   */

  for (auto row_idx = size_t{0}; row_idx < customer_count; row_idx++) {
    auto customer = call_dbgen_mk<customer_t>(row_idx + 1, mk_cust, TPCHTable::Customer);
    customer_builder.append_row(customer.custkey, customer.name, customer.address, customer.nation_code, customer.phone,
                                convert_money(customer.acctbal), customer.mktsegment, customer.comment);
  }

  /**
   * ORDER and LINEITEM
   */

  for (auto order_idx = size_t{0}; order_idx < order_count; ++order_idx) {
    const auto order = call_dbgen_mk<order_t>(order_idx + 1, mk_order, TPCHTable::Orders, 0l);

    order_builder.append_row(order.okey, order.custkey, pmr_string(1, order.orderstatus),
                             convert_money(order.totalprice), order.odate, order.opriority, order.clerk,
                             order.spriority, order.comment);

    for (auto line_idx = int64_t{0}; line_idx < order.lines; ++line_idx) {
      const auto& lineitem = order.l[line_idx];

      lineitem_builder.append_row(lineitem.okey, lineitem.partkey, lineitem.suppkey, lineitem.lcnt, lineitem.quantity,
                                  convert_money(lineitem.eprice), convert_money(lineitem.discount),
                                  convert_money(lineitem.tax), pmr_string(1, lineitem.rflag[0]),
                                  pmr_string(1, lineitem.lstatus[0]), lineitem.sdate, lineitem.cdate, lineitem.rdate,
                                  lineitem.shipinstruct, lineitem.shipmode, lineitem.comment);
    }
  }

  /**
   * PART and PARTSUPP
   */

  for (auto part_idx = size_t{0}; part_idx < part_count; ++part_idx) {
    const auto part = call_dbgen_mk<part_t>(part_idx + 1, mk_part, TPCHTable::Part);

    part_builder.append_row(part.partkey, part.name, part.mfgr, part.brand, part.type, part.size, part.container,
                            convert_money(part.retailprice), part.comment);

    // Some scale factors (e.g., 0.05) are not supported by tpch-dbgen as they produce non-unique partkey/suppkey
    // combinations. The reason is probably somewhere in the magic in PART_SUPP_BRIDGE. As the partkey is
    // ascending, those are easy to identify:

    DSS_HUGE last_partkey = {};
    auto suppkeys = std::vector<DSS_HUGE>{};

    for (const auto& partsupp : part.s) {
      {
        // Make sure we do not generate non-unique combinations (see above)
        if (partsupp.partkey != last_partkey) {
          Assert(partsupp.partkey > last_partkey, "Expected partkey to be generated in ascending order.");
          last_partkey = partsupp.partkey;
          suppkeys.clear();
        }
        Assert(std::find(suppkeys.begin(), suppkeys.end(), partsupp.suppkey) == suppkeys.end(),
               "Scale factor unsupported by tpch-dbgen. Consider choosing a \"round\" number.");
        suppkeys.emplace_back(partsupp.suppkey);
      }

      partsupp_builder.append_row(partsupp.partkey, partsupp.suppkey, partsupp.qty, convert_money(partsupp.scost),
                                  partsupp.comment);
    }
  }

  /**
   * SUPPLIER
   */

  for (auto supplier_idx = size_t{0}; supplier_idx < supplier_count; ++supplier_idx) {
    const auto supplier = call_dbgen_mk<supplier_t>(supplier_idx + 1, mk_supp, TPCHTable::Supplier);

    supplier_builder.append_row(supplier.suppkey, supplier.name, supplier.address, supplier.nation_code, supplier.phone,
                                convert_money(supplier.acctbal), supplier.comment);
  }

  /**
   * NATION
   */

  for (auto nation_idx = size_t{0}; nation_idx < nation_count; ++nation_idx) {
    const auto nation = call_dbgen_mk<code_t>(nation_idx + 1, mk_nation, TPCHTable::Nation);
    nation_builder.append_row(nation.code, nation.text, nation.join, nation.comment);
  }

  /**
   * REGION
   */

  for (auto region_idx = size_t{0}; region_idx < region_count; ++region_idx) {
    const auto region = call_dbgen_mk<code_t>(region_idx + 1, mk_region, TPCHTable::Region);
    region_builder.append_row(region.code, region.text, region.comment);
  }

  /**
   * Clean up dbgen every time we finish table generation to avoid memory leaks in dbgen
   */
  dbgen_cleanup();

  /**
   * Return
   */
  std::unordered_map<std::string, BenchmarkTableInfo> table_info_by_name;

  auto customer_table = customer_builder.finish_table();
  table_info_by_name["customer"].table = customer_table;

  auto orders_table = order_builder.finish_table();
  table_info_by_name["orders"].table = orders_table;

  auto lineitem_table = lineitem_builder.finish_table();
  table_info_by_name["lineitem"].table = lineitem_table;

  auto part_table = part_builder.finish_table();
  table_info_by_name["part"].table = part_table;

  auto partsupp_table = partsupp_builder.finish_table();
  table_info_by_name["partsupp"].table = partsupp_table;

  auto supplier_table = supplier_builder.finish_table();
  table_info_by_name["supplier"].table = supplier_table;

  auto nation_table = nation_builder.finish_table();
  table_info_by_name["nation"].table = nation_table;

  auto region_table = region_builder.finish_table();
  table_info_by_name["region"].table = region_table;

  if (_benchmark_config->cache_binary_tables) {
    std::filesystem::create_directories(cache_directory);
    for (auto& [table_name, table_info] : table_info_by_name) {
      table_info.binary_file_path = cache_directory + "/" + table_name + ".bin";  // NOLINT
    }
  }

  return table_info_by_name;
}

AbstractTableGenerator::IndexesByTable TPCHTableGenerator::_indexes_by_table() const {
  return {{"part", {{"p_partkey"}}},
          {"supplier", {{"s_suppkey"}, {"s_nationkey"}}},
          {"partsupp", {{"ps_partkey"}, {"ps_suppkey"}}},
          {"customer", {{"c_custkey"}, {"c_nationkey"}}},
          {"orders", {{"o_orderkey"}, {"o_custkey"}}},
          {"lineitem", {{"l_orderkey"}, {"l_partkey"}}},
          {"nation", {{"n_nationkey"}, {"n_regionkey"}}},
          {"region", {{"r_regionkey"}}}};
}

AbstractTableGenerator::SortOrderByTable TPCHTableGenerator::_sort_order_by_table() const {
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

void TPCHTableGenerator::_add_constraints(
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
