#include "tpch_table_generator.hpp"

extern "C" {
#include <dss.h>
#include <dsstypes.h>
#include <rnd.h>
}

#include <filesystem>
#include <utility>

#include "benchmark_config.hpp"
#include "operators/import_binary.hpp"
#include "storage/chunk.hpp"
#include "table_builder.hpp"
#include "utils/timer.hpp"

extern char** asc_date;
extern seed_t seed[];

#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#pragma clang diagnostic ignored "-Wfloat-conversion"

namespace {

using namespace opossum;  // NOLINT

// clang-format off
const auto customer_column_types = boost::hana::tuple      <int32_t,    pmr_string,  pmr_string,  int32_t,       pmr_string,  float,       pmr_string,     pmr_string>();  // NOLINT
const auto customer_column_names = boost::hana::make_tuple("c_custkey", "c_name",    "c_address", "c_nationkey", "c_phone",   "c_acctbal", "c_mktsegment", "c_comment"); // NOLINT

const auto order_column_types = boost::hana::tuple      <int32_t,     int32_t,     pmr_string,      float,          pmr_string,    pmr_string,        pmr_string,  int32_t,          pmr_string>();  // NOLINT
const auto order_column_names = boost::hana::make_tuple("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk",   "o_shippriority", "o_comment");  // NOLINT

const auto lineitem_column_types = boost::hana::tuple      <int32_t,     int32_t,     int32_t,     int32_t,        float,        float,             float,        float,   pmr_string,     pmr_string,     pmr_string,   pmr_string,     pmr_string,      pmr_string,       pmr_string,   pmr_string>();  // NOLINT
const auto lineitem_column_names = boost::hana::make_tuple("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment");  // NOLINT

const auto part_column_types = boost::hana::tuple      <int32_t,    pmr_string,  pmr_string,  pmr_string,  pmr_string,  int32_t,  pmr_string,    float,        pmr_string>();  // NOLINT
const auto part_column_names = boost::hana::make_tuple("p_partkey", "p_name",    "p_mfgr",    "p_brand",   "p_type",    "p_size", "p_container", "p_retailsize", "p_comment");  // NOLINT

const auto partsupp_column_types = boost::hana::tuple<     int32_t,      int32_t,      int32_t,       float,           pmr_string>();  // NOLINT
const auto partsupp_column_names = boost::hana::make_tuple("ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment");  // NOLINT

const auto supplier_column_types = boost::hana::tuple<     int32_t,     pmr_string,  pmr_string,  int32_t,       pmr_string,  float,       pmr_string>();  // NOLINT
const auto supplier_column_names = boost::hana::make_tuple("s_suppkey", "s_name",    "s_address", "s_nationkey", "s_phone",   "s_acctbal", "s_comment");  // NOLINT

const auto nation_column_types = boost::hana::tuple<     int32_t,       pmr_string,  int32_t,       pmr_string>();  // NOLINT
const auto nation_column_names = boost::hana::make_tuple("n_nationkey", "n_name",    "n_regionkey", "n_comment");  // NOLINT

const auto region_column_types = boost::hana::tuple<     int32_t,       pmr_string,  pmr_string>();  // NOLINT
const auto region_column_names = boost::hana::make_tuple("r_regionkey", "r_name",    "r_comment");  // NOLINT
// clang-format on

std::unordered_map<opossum::TPCHTable, std::underlying_type_t<opossum::TPCHTable>> tpch_table_to_dbgen_id = {
    {opossum::TPCHTable::Part, PART},     {opossum::TPCHTable::PartSupp, PSUPP}, {opossum::TPCHTable::Supplier, SUPP},
    {opossum::TPCHTable::Customer, CUST}, {opossum::TPCHTable::Orders, ORDER},   {opossum::TPCHTable::LineItem, LINE},
    {opossum::TPCHTable::Nation, NATION}, {opossum::TPCHTable::Region, REGION}};

template <typename DSSType, typename MKRetType, typename... Args>
DSSType call_dbgen_mk(size_t idx, MKRetType (*mk_fn)(DSS_HUGE, DSSType* val, Args...), opossum::TPCHTable table,
                      Args... args) {
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
  return dollars + (static_cast<float>(cents)) / 100.0f;
}

/**
 * Call this after using dbgen to avoid memory leaks
 */
void dbgen_cleanup() {
  for (auto* distribution : {&nations,     &regions,        &o_priority_set, &l_instruct_set,
                             &l_smode_set, &l_category_set, &l_rflag_set,    &c_mseg_set,
                             &colors,      &p_types_set,    &p_cntr_set,     &articles,
                             &nouns,       &adjectives,     &adverbs,        &prepositions,
                             &verbs,       &terminators,    &auxillaries,    &np,
                             &vp,          &grammar}) {
    free(distribution->permute);  // NOLINT
    distribution->permute = nullptr;
  }

  if (asc_date) {
    for (size_t idx = 0; idx < TOTDATE; ++idx) {
      free(asc_date[idx]);  // NOLINT
    }
    free(asc_date);  // NOLINT
  }
  asc_date = nullptr;
}

}  // namespace

namespace opossum {

std::unordered_map<TPCHTable, std::string> tpch_table_names = {
    {TPCHTable::Part, "part"},         {TPCHTable::PartSupp, "partsupp"}, {TPCHTable::Supplier, "supplier"},
    {TPCHTable::Customer, "customer"}, {TPCHTable::Orders, "orders"},     {TPCHTable::LineItem, "lineitem"},
    {TPCHTable::Nation, "nation"},     {TPCHTable::Region, "region"}};

TPCHTableGenerator::TPCHTableGenerator(float scale_factor, uint32_t chunk_size)
    : AbstractTableGenerator(create_benchmark_config_with_chunk_size(chunk_size)), _scale_factor(scale_factor) {}

TPCHTableGenerator::TPCHTableGenerator(float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator(benchmark_config), _scale_factor(scale_factor) {}

std::unordered_map<std::string, BenchmarkTableInfo> TPCHTableGenerator::generate() {
  Assert(_scale_factor < 1.0f || std::round(_scale_factor) == _scale_factor,
         "Due to tpch_dbgen limitations, only scale factors less than one can have a fractional part.");

  const auto cache_directory = std::string{"tpch_cached_tables/sf-"} + std::to_string(_scale_factor);  // NOLINT
  if (_benchmark_config->cache_binary_tables && std::filesystem::is_directory(cache_directory)) {
    std::unordered_map<std::string, BenchmarkTableInfo> table_info_by_name;

    for (const auto& table_file : std::filesystem::recursive_directory_iterator(cache_directory)) {
      const auto table_name = table_file.path().stem();
      Timer timer;
      std::cout << "-  Loading table " << table_name << " from cached binary " << table_file.path().relative_path();

      BenchmarkTableInfo table_info;
      table_info.table = ImportBinary::read_binary(table_file.path());
      table_info.loaded_from_binary = true;
      table_info_by_name[table_name] = table_info;

      std::cout << " (" << timer.lap_formatted() << ")" << std::endl;
    }

    return table_info_by_name;
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
  TableBuilder customer_builder{_benchmark_config->chunk_size, customer_column_types, customer_column_names,
                                customer_count};
  TableBuilder order_builder{_benchmark_config->chunk_size, order_column_types, order_column_names, order_count};
  TableBuilder lineitem_builder{_benchmark_config->chunk_size, lineitem_column_types, lineitem_column_names,
                                order_count * 4};
  TableBuilder part_builder{_benchmark_config->chunk_size, part_column_types, part_column_names, part_count};
  TableBuilder partsupp_builder{_benchmark_config->chunk_size, partsupp_column_types, partsupp_column_names,
                                part_count * 4};
  TableBuilder supplier_builder{_benchmark_config->chunk_size, supplier_column_types, supplier_column_names,
                                supplier_count};
  TableBuilder nation_builder{_benchmark_config->chunk_size, nation_column_types, nation_column_names, nation_count};
  TableBuilder region_builder{_benchmark_config->chunk_size, region_column_types, region_column_names, region_count};

  /**
   * CUSTOMER
   */

  for (size_t row_idx = 0; row_idx < customer_count; row_idx++) {
    auto customer = call_dbgen_mk<customer_t>(row_idx + 1, mk_cust, TPCHTable::Customer);
    customer_builder.append_row(customer.custkey, customer.name, customer.address, customer.nation_code, customer.phone,
                                convert_money(customer.acctbal), customer.mktsegment, customer.comment);
  }

  /**
   * ORDER and LINEITEM
   */

  for (size_t order_idx = 0; order_idx < order_count; ++order_idx) {
    const auto order = call_dbgen_mk<order_t>(order_idx + 1, mk_order, TPCHTable::Orders, 0l);

    order_builder.append_row(order.okey, order.custkey, pmr_string(1, order.orderstatus),
                             convert_money(order.totalprice), order.odate, order.opriority, order.clerk,
                             order.spriority, order.comment);

    for (auto line_idx = 0; line_idx < order.lines; ++line_idx) {
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

  for (size_t part_idx = 0; part_idx < part_count; ++part_idx) {
    const auto part = call_dbgen_mk<part_t>(part_idx + 1, mk_part, TPCHTable::Part);

    part_builder.append_row(part.partkey, part.name, part.mfgr, part.brand, part.type, part.size, part.container,
                            convert_money(part.retailprice), part.comment);

    for (const auto& partsupp : part.s) {
      partsupp_builder.append_row(partsupp.partkey, partsupp.suppkey, partsupp.qty, convert_money(partsupp.scost),
                                  partsupp.comment);
    }
  }

  /**
   * SUPPLIER
   */

  for (size_t supplier_idx = 0; supplier_idx < supplier_count; ++supplier_idx) {
    const auto supplier = call_dbgen_mk<supplier_t>(supplier_idx + 1, mk_supp, TPCHTable::Supplier);

    supplier_builder.append_row(supplier.suppkey, supplier.name, supplier.address, supplier.nation_code, supplier.phone,
                                convert_money(supplier.acctbal), supplier.comment);
  }

  /**
   * NATION
   */

  for (size_t nation_idx = 0; nation_idx < nation_count; ++nation_idx) {
    const auto nation = call_dbgen_mk<code_t>(nation_idx + 1, mk_nation, TPCHTable::Nation);
    nation_builder.append_row(nation.code, nation.text, nation.join, nation.comment);
  }

  /**
   * REGION
   */

  for (size_t region_idx = 0; region_idx < region_count; ++region_idx) {
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
  customer_table->add_soft_unique_constraint({customer_table->column_id_by_name("c_custkey")}, true);
  table_info_by_name["customer"].table = customer_table;

  auto orders_table = order_builder.finish_table();
  orders_table->add_soft_unique_constraint({orders_table->column_id_by_name("o_orderkey")}, true);
  table_info_by_name["orders"].table = orders_table;

  auto lineitem_table = lineitem_builder.finish_table();
  lineitem_table->add_soft_unique_constraint(
      {lineitem_table->column_id_by_name("l_orderkey"), lineitem_table->column_id_by_name("l_linenumber")}, true);
  table_info_by_name["lineitem"].table = lineitem_table;

  auto part_table = part_builder.finish_table();
  part_table->add_soft_unique_constraint({part_table->column_id_by_name("p_partkey")}, true);
  table_info_by_name["part"].table = part_table;

  auto partsupp_table = partsupp_builder.finish_table();
  partsupp_table->add_soft_unique_constraint(
      {partsupp_table->column_id_by_name("ps_partkey"), partsupp_table->column_id_by_name("ps_suppkey")}, true);
  table_info_by_name["partsupp"].table = partsupp_table;

  auto supplier_table = supplier_builder.finish_table();
  supplier_table->add_soft_unique_constraint({supplier_table->column_id_by_name("s_suppkey")}, true);
  table_info_by_name["supplier"].table = supplier_table;

  auto nation_table = nation_builder.finish_table();
  nation_table->add_soft_unique_constraint({nation_table->column_id_by_name("n_nationkey")}, true);
  table_info_by_name["nation"].table = nation_table;

  auto region_table = region_builder.finish_table();
  region_table->add_soft_unique_constraint({region_table->column_id_by_name("r_regionkey")}, true);
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
  return {
      {"part", {{"p_partkey"}}},
      {"supplier", {{"s_suppkey"}, {"s_nationkey"}}},
      // TODO(anyone): multi-column indexes are currently not used by the index scan rule and the translator
      {"partsupp", {{"ps_partkey", "ps_suppkey"}, {"ps_suppkey"}}},  // ps_partkey is subset of {ps_partkey, ps_suppkey}
      {"customer", {{"c_custkey"}, {"c_nationkey"}}},
      {"orders", {{"o_orderkey"}, {"o_custkey"}}},
      {"lineitem", {{"l_orderkey", "l_linenumber"}, {"l_partkey", "l_suppkey"}}},
      {"nation", {{"n_nationkey"}, {"n_regionkey"}}},
      {"region", {{"r_regionkey"}}},
  };
}

AbstractTableGenerator::SortOrderByTable TPCHTableGenerator::_sort_order_by_table() const {
  // Allowed as per TPC-H Specification, paragraph 1.5.2
  return {{"lineitem", "l_shipdate"}, {"orders", "o_orderdate"}};
}

}  // namespace opossum
