#include "tpch_table_generator.hpp"

extern "C" {
#include <dss.h>
#include <dsstypes.h>
#include <rnd.h>
}

#include <utility>

#include "table_builder.hpp"
#include "benchmark_config.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"

extern char** asc_date;
extern seed_t seed[];

#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#pragma clang diagnostic ignored "-Wfloat-conversion"

namespace {

using namespace opossum;  // NOLINT

const auto customer_definitions = hana::make_tuple(hana::make_tuple("c_custkey", hana::type_c<int32_t>, false),
                                                   hana::make_tuple("c_name", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("c_address", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("c_nationkey", hana::type_c<int32_t>, false),
                                                   hana::make_tuple("c_phone", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("c_acctbal", hana::type_c<float>, false),
                                                   hana::make_tuple("c_mktsegment", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("c_comment", hana::type_c<pmr_string>, false));

const auto order_definitions = hana::make_tuple(hana::make_tuple("o_orderkey", hana::type_c<int32_t>, false),
                                                hana::make_tuple("o_custkey", hana::type_c<int32_t>, false),
                                                hana::make_tuple("o_orderstatus", hana::type_c<pmr_string>, false),
                                                hana::make_tuple("o_totalprice", hana::type_c<float>, false),
                                                hana::make_tuple("o_orderdate", hana::type_c<pmr_string>, false),
                                                hana::make_tuple("o_orderpriority", hana::type_c<pmr_string>, false),
                                                hana::make_tuple("o_clerk", hana::type_c<pmr_string>, false),
                                                hana::make_tuple("o_shippriority", hana::type_c<int32_t>, false),
                                                hana::make_tuple("o_comment", hana::type_c<pmr_string>, false));

const auto lineitem_definitions = hana::make_tuple(hana::make_tuple("l_orderkey", hana::type_c<int32_t>, false),
                                                   hana::make_tuple("l_partkey", hana::type_c<int32_t>, false),
                                                   hana::make_tuple("l_suppkey", hana::type_c<int32_t>, false),
                                                   hana::make_tuple("l_linenumber", hana::type_c<int32_t>, false),
                                                   hana::make_tuple("l_quantity", hana::type_c<float>, false),
                                                   hana::make_tuple("l_extendedprice", hana::type_c<float>, false),
                                                   hana::make_tuple("l_discount", hana::type_c<float>, false),
                                                   hana::make_tuple("l_tax", hana::type_c<float>, false),
                                                   hana::make_tuple("l_returnflag", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("l_linestatus", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("l_shipdate", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("l_commitdate", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("l_receiptdate", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("l_shipinstruct", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("l_shipmode", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("l_comment", hana::type_c<pmr_string>, false));

const auto part_definitions = hana::make_tuple(hana::make_tuple("p_partkey", hana::type_c<int32_t>, false),
                                               hana::make_tuple("p_name", hana::type_c<pmr_string>, false),
                                               hana::make_tuple("p_mfgr", hana::type_c<pmr_string>, false),
                                               hana::make_tuple("p_brand", hana::type_c<pmr_string>, false),
                                               hana::make_tuple("p_type", hana::type_c<pmr_string>, false),
                                               hana::make_tuple("p_size", hana::type_c<int32_t>, false),
                                               hana::make_tuple("p_container", hana::type_c<pmr_string>, false),
                                               hana::make_tuple("p_retailsize", hana::type_c<float>, false),
                                               hana::make_tuple("p_comment", hana::type_c<pmr_string>, false));

const auto partsupp_definitions = hana::make_tuple(hana::make_tuple("ps_partkey", hana::type_c<int32_t>, false),
                                                   hana::make_tuple("ps_suppkey", hana::type_c<int32_t>, false),
                                                   hana::make_tuple("ps_availqty", hana::type_c<int32_t>, false),
                                                   hana::make_tuple("ps_supplycost", hana::type_c<float>, false),
                                                   hana::make_tuple("ps_comment", hana::type_c<pmr_string>, false));

const auto supplier_definitions = hana::make_tuple(hana::make_tuple("s_suppkey", hana::type_c<int32_t>, false),
                                                   hana::make_tuple("s_name", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("s_address", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("s_nationkey", hana::type_c<int32_t>, false),
                                                   hana::make_tuple("s_phone", hana::type_c<pmr_string>, false),
                                                   hana::make_tuple("s_acctbal", hana::type_c<float>, false),
                                                   hana::make_tuple("s_comment", hana::type_c<pmr_string>, false));

const auto nation_definitions = hana::make_tuple(hana::make_tuple("n_nationkey", hana::type_c<int32_t>, false),
                                                 hana::make_tuple("n_name", hana::type_c<pmr_string>, false),
                                                 hana::make_tuple("n_regionkey", hana::type_c<int32_t>, false),
                                                 hana::make_tuple("n_comment", hana::type_c<pmr_string>, false));

const auto region_definitions = hana::make_tuple(hana::make_tuple("r_regionkey", hana::type_c<int32_t>, false),
                                                 hana::make_tuple("r_name", hana::type_c<pmr_string>, false),
                                                 hana::make_tuple("r_comment", hana::type_c<pmr_string>, false));

std::unordered_map<opossum::TpchTable, std::underlying_type_t<opossum::TpchTable>> tpch_table_to_dbgen_id = {
    {opossum::TpchTable::Part, PART},     {opossum::TpchTable::PartSupp, PSUPP}, {opossum::TpchTable::Supplier, SUPP},
    {opossum::TpchTable::Customer, CUST}, {opossum::TpchTable::Orders, ORDER},   {opossum::TpchTable::LineItem, LINE},
    {opossum::TpchTable::Nation, NATION}, {opossum::TpchTable::Region, REGION}};

template <typename DSSType, typename MKRetType, typename... Args>
DSSType call_dbgen_mk(size_t idx, MKRetType (*mk_fn)(DSS_HUGE, DSSType* val, Args...), opossum::TpchTable table,
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

std::shared_ptr<BenchmarkConfig> create_benchmark_config_with_chunk_size(uint32_t chunk_size) {
  auto config = BenchmarkConfig::get_default_config();
  config.chunk_size = chunk_size;
  return std::make_shared<BenchmarkConfig>(config);
}

}  // namespace

namespace opossum {

std::unordered_map<TpchTable, std::string> tpch_table_names = {
    {TpchTable::Part, "part"},         {TpchTable::PartSupp, "partsupp"}, {TpchTable::Supplier, "supplier"},
    {TpchTable::Customer, "customer"}, {TpchTable::Orders, "orders"},     {TpchTable::LineItem, "lineitem"},
    {TpchTable::Nation, "nation"},     {TpchTable::Region, "region"}};

TpchTableGenerator::TpchTableGenerator(float scale_factor, uint32_t chunk_size)
    : AbstractTableGenerator(create_benchmark_config_with_chunk_size(chunk_size)), _scale_factor(scale_factor) {}

TpchTableGenerator::TpchTableGenerator(float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator(benchmark_config), _scale_factor(scale_factor) {}

std::unordered_map<std::string, BenchmarkTableInfo> TpchTableGenerator::generate() {
  Assert(_scale_factor < 1.0f || std::round(_scale_factor) == _scale_factor,
         "Due to tpch_dbgen limitations, only scale factors less than one can have a fractional part.");
  Assert(!_benchmark_config->cache_binary_tables, "Caching binary Tables not supported by TpchTableGenerator, yet");

  // Init tpch_dbgen - it is important this is done before any data structures from tpch_dbgen are read.
  dbgen_reset_seeds();
  dbgen_init_scale_factor(_scale_factor);

  const auto customer_count = static_cast<size_t>(tdefs[CUST].base * scale);
  const auto order_count = static_cast<size_t>(tdefs[ORDER].base * scale);
  const auto part_count = static_cast<size_t>(tdefs[PART].base * scale);
  const auto supplier_count = static_cast<size_t>(tdefs[SUPP].base * scale);
  const auto nation_count = static_cast<size_t>(tdefs[NATION].base);
  const auto region_count = static_cast<size_t>(tdefs[REGION].base);

  // The `* 4` part is defined in the TPC-H specification.
  TableBuilder customer_builder{_benchmark_config->chunk_size, customer_definitions, UseMvcc::Yes, customer_count};
  TableBuilder order_builder{_benchmark_config->chunk_size, order_definitions, UseMvcc::Yes, order_count};
  TableBuilder lineitem_builder{_benchmark_config->chunk_size, lineitem_definitions, UseMvcc::Yes, order_count * 4};
  TableBuilder part_builder{_benchmark_config->chunk_size, part_definitions, UseMvcc::Yes, part_count};
  TableBuilder partsupp_builder{_benchmark_config->chunk_size, partsupp_definitions, UseMvcc::Yes, part_count * 4};
  TableBuilder supplier_builder{_benchmark_config->chunk_size, supplier_definitions, UseMvcc::Yes, supplier_count};
  TableBuilder nation_builder{_benchmark_config->chunk_size, nation_definitions, UseMvcc::Yes, nation_count};
  TableBuilder region_builder{_benchmark_config->chunk_size, region_definitions, UseMvcc::Yes, region_count};

  /**
   * CUSTOMER
   */

  for (size_t row_idx = 0; row_idx < customer_count; row_idx++) {
    auto customer = call_dbgen_mk<customer_t>(row_idx + 1, mk_cust, TpchTable::Customer);
    customer_builder.append_row(customer.custkey, customer.name, customer.address, customer.nation_code, customer.phone,
                                convert_money(customer.acctbal), customer.mktsegment, customer.comment);
  }

  /**
   * ORDER and LINEITEM
   */

  for (size_t order_idx = 0; order_idx < order_count; ++order_idx) {
    const auto order = call_dbgen_mk<order_t>(order_idx + 1, mk_order, TpchTable::Orders, 0l);

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
    const auto part = call_dbgen_mk<part_t>(part_idx + 1, mk_part, TpchTable::Part);

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
    const auto supplier = call_dbgen_mk<supplier_t>(supplier_idx + 1, mk_supp, TpchTable::Supplier);

    supplier_builder.append_row(supplier.suppkey, supplier.name, supplier.address, supplier.nation_code, supplier.phone,
                                convert_money(supplier.acctbal), supplier.comment);
  }

  /**
   * NATION
   */

  for (size_t nation_idx = 0; nation_idx < nation_count; ++nation_idx) {
    const auto nation = call_dbgen_mk<code_t>(nation_idx + 1, mk_nation, TpchTable::Nation);
    nation_builder.append_row(nation.code, nation.text, nation.join, nation.comment);
  }

  /**
   * REGION
   */

  for (size_t region_idx = 0; region_idx < region_count; ++region_idx) {
    const auto region = call_dbgen_mk<code_t>(region_idx + 1, mk_region, TpchTable::Region);
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

  table_info_by_name["customer"].table = customer_builder.finish_table();
  table_info_by_name["orders"].table = order_builder.finish_table();
  table_info_by_name["lineitem"].table = lineitem_builder.finish_table();
  table_info_by_name["part"].table = part_builder.finish_table();
  table_info_by_name["partsupp"].table = partsupp_builder.finish_table();
  table_info_by_name["supplier"].table = supplier_builder.finish_table();
  table_info_by_name["nation"].table = nation_builder.finish_table();
  table_info_by_name["region"].table = region_builder.finish_table();

  return table_info_by_name;
}

}  // namespace opossum
