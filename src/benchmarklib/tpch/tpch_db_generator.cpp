#include "tpch_db_generator.hpp"

extern "C" {
#include <dss.h>
#include <dsstypes.h>
#include <rnd.h>
}

#include <utility>

#include "boost/hana/for_each.hpp"
#include "boost/hana/integral_constant.hpp"
#include "boost/hana/zip_with.hpp"

#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"

extern char** asc_date;
extern seed_t seed[];

namespace {

// clang-format off
const auto customer_column_types = boost::hana::tuple      <int32_t,    std::string, std::string, int32_t,       std::string, float,       std::string,    std::string>();  // NOLINT
const auto customer_column_names = boost::hana::make_tuple("c_custkey", "c_name",    "c_address", "c_nationkey", "c_phone",   "c_acctbal", "c_mktsegment", "c_comment"); // NOLINT

const auto order_column_types = boost::hana::tuple      <int32_t,     int32_t,     std::string,     float,          std::string,   std::string,       std::string, int32_t,          std::string>();  // NOLINT
const auto order_column_names = boost::hana::make_tuple("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk",   "o_shippriority", "o_comment");  // NOLINT

const auto lineitem_column_types = boost::hana::tuple      <int32_t,     int32_t,     int32_t,     int32_t,        float,        float,             float,        float,   std::string,    std::string,    std::string,  std::string,    std::string,     std::string,      std::string,  std::string>();  // NOLINT
const auto lineitem_column_names = boost::hana::make_tuple("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment");  // NOLINT

const auto part_column_types = boost::hana::tuple      <int32_t,    std::string, std::string, std::string, std::string, int32_t,  std::string,   float,        std::string>();  // NOLINT
const auto part_column_names = boost::hana::make_tuple("p_partkey", "p_name",    "p_mfgr",    "p_brand",   "p_type",    "p_size", "p_container", "p_retailsize", "p_comment");  // NOLINT

const auto partsupp_column_types = boost::hana::tuple<     int32_t,      int32_t,      int32_t,       float,           std::string>();  // NOLINT
const auto partsupp_column_names = boost::hana::make_tuple("ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment");  // NOLINT

const auto supplier_column_types = boost::hana::tuple<     int32_t,     std::string, std::string, int32_t,       std::string, float,       std::string>();  // NOLINT
const auto supplier_column_names = boost::hana::make_tuple("s_suppkey", "s_name",    "s_address", "s_nationkey", "s_phone",   "s_acctbal", "s_comment");  // NOLINT

const auto nation_column_types = boost::hana::tuple<     int32_t,       std::string, int32_t,       std::string>();  // NOLINT
const auto nation_column_names = boost::hana::make_tuple("n_nationkey", "n_name",    "n_regionkey", "n_comment");  // NOLINT

const auto region_column_types = boost::hana::tuple<     int32_t,       std::string, std::string>();  // NOLINT
const auto region_column_names = boost::hana::make_tuple("r_regionkey", "r_name",    "r_comment");  // NOLINT

// clang-format on

/**
 * Helper to build a table with a static (specified by template args `ColumnTypes`) column type layout. Keeps a vector
 * for each column and appends values to them in append_row(). Automatically creates chunks in accordance with the
 * specified chunk size.
 *
 * No real need to tie this to TPCH, but atm it is only used here so that's where it resides.
 */
template <typename... DataTypes>
class TableBuilder {
 public:
  template <typename... Strings>
  TableBuilder(size_t chunk_size, const boost::hana::tuple<DataTypes...>& column_types,
               const boost::hana::tuple<Strings...>& column_names, opossum::UseMvcc use_mvcc, size_t estimated_rows = 0)
      : _use_mvcc(use_mvcc), _estimated_rows_per_chunk(estimated_rows < chunk_size ? estimated_rows : chunk_size) {
    /**
     * Create a tuple ((column_name0, column_type0), (column_name1, column_type1), ...) so we can iterate over the
     * columns.
     * fold_left as below does this in order, I think boost::hana::zip_with() doesn't, which is why I'm doing two steps
     * here.
     */
    const auto column_names_and_data_types = boost::hana::zip_with(
        [&](auto column_type, auto column_name) {
          return boost::hana::make_tuple(column_name, opossum::data_type_from_type<decltype(column_type)>());
        },
        column_types, column_names);

    // Iterate over the column types/names and create the columns.
    opossum::TableColumnDefinitions column_definitions;
    boost::hana::fold_left(column_names_and_data_types, column_definitions,
                           [](auto& column_definitions, auto column_name_and_type) -> decltype(auto) {
                             column_definitions.emplace_back(column_name_and_type[boost::hana::llong_c<0>],
                                                             column_name_and_type[boost::hana::llong_c<1>]);
                             return column_definitions;
                           });
    _table = std::make_shared<opossum::Table>(column_definitions, opossum::TableType::Data, chunk_size, use_mvcc);

    // Reserve some space in the vectors
    boost::hana::for_each(_data_vectors, [&](auto&& vector) { vector.reserve(_estimated_rows_per_chunk); });
  }

  std::shared_ptr<opossum::Table> finish_table() {
    if (_current_chunk_row_count() > 0) {
      _emit_chunk();
    }

    return _table;
  }

  void append_row(DataTypes&&... column_values) {
    // Create a tuple ([&data_vector0, value0], ...)
    auto vectors_and_values = boost::hana::zip_with(
        [](auto& vector, auto&& value) { return boost::hana::make_tuple(std::reference_wrapper(vector), value); },
        _data_vectors, boost::hana::make_tuple(std::forward<DataTypes>(column_values)...));

    // Add the values to their respective data vector
    boost::hana::for_each(vectors_and_values, [](auto vector_and_value) {
      vector_and_value[boost::hana::llong_c<0>].get().push_back(vector_and_value[boost::hana::llong_c<1>]);
    });

    if (_current_chunk_row_count() >= _table->max_chunk_size()) {
      _emit_chunk();
    }
  }

 private:
  std::shared_ptr<opossum::Table> _table;
  opossum::UseMvcc _use_mvcc;
  boost::hana::tuple<opossum::pmr_concurrent_vector<DataTypes>...> _data_vectors;
  size_t _estimated_rows_per_chunk;

  size_t _current_chunk_row_count() const { return _data_vectors[boost::hana::llong_c<0>].size(); }

  void _emit_chunk() {
    opossum::Segments segments;

    // Create a segment from each data vector and add it to the Chunk, then re-initialize the vector
    boost::hana::for_each(_data_vectors, [&](auto&& vector) {
      using T = typename std::decay_t<decltype(vector)>::value_type;
      // reason for nolint: clang-tidy wants this to be a forward, but that doesn't work
      segments.push_back(std::make_shared<opossum::ValueSegment<T>>(std::move(vector)));  // NOLINT
      vector = std::decay_t<decltype(vector)>();
      vector.reserve(_estimated_rows_per_chunk);
    });
    _table->append_chunk(segments);
  }
};

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

  if (asc_date != nullptr) {
    for (size_t idx = 0; idx < TOTDATE; ++idx) {
      free(asc_date[idx]);  // NOLINT
    }
    free(asc_date);  // NOLINT
  }
  asc_date = nullptr;
}

}  // namespace

namespace opossum {

std::unordered_map<TpchTable, std::string> tpch_table_names = {
    {TpchTable::Part, "part"},         {TpchTable::PartSupp, "partsupp"}, {TpchTable::Supplier, "supplier"},
    {TpchTable::Customer, "customer"}, {TpchTable::Orders, "orders"},     {TpchTable::LineItem, "lineitem"},
    {TpchTable::Nation, "nation"},     {TpchTable::Region, "region"}};

TpchDbGenerator::TpchDbGenerator(float scale_factor, uint32_t chunk_size)
    : _scale_factor(scale_factor), _chunk_size(chunk_size) {}

std::unordered_map<TpchTable, std::shared_ptr<Table>> TpchDbGenerator::generate() {
  const auto customer_count = static_cast<size_t>(tdefs[CUST].base * _scale_factor);
  const auto order_count = static_cast<size_t>(tdefs[ORDER].base * _scale_factor);
  const auto part_count = static_cast<size_t>(tdefs[PART].base * _scale_factor);
  const auto supplier_count = static_cast<size_t>(tdefs[SUPP].base * _scale_factor);
  const auto nation_count = static_cast<size_t>(tdefs[NATION].base);
  const auto region_count = static_cast<size_t>(tdefs[REGION].base);

  // The `* 4` part is defined in the TPC-H specification.
  TableBuilder customer_builder{_chunk_size, customer_column_types, customer_column_names, UseMvcc::Yes,
                                customer_count};
  TableBuilder order_builder{_chunk_size, order_column_types, order_column_names, UseMvcc::Yes, order_count};
  TableBuilder lineitem_builder{_chunk_size, lineitem_column_types, lineitem_column_names, UseMvcc::Yes,
                                order_count * 4};
  TableBuilder part_builder{_chunk_size, part_column_types, part_column_names, UseMvcc::Yes, part_count};
  TableBuilder partsupp_builder{_chunk_size, partsupp_column_types, partsupp_column_names, UseMvcc::Yes,
                                part_count * 4};
  TableBuilder supplier_builder{_chunk_size, supplier_column_types, supplier_column_names, UseMvcc::Yes,
                                supplier_count};
  TableBuilder nation_builder{_chunk_size, nation_column_types, nation_column_names, UseMvcc::Yes, nation_count};
  TableBuilder region_builder{_chunk_size, region_column_types, region_column_names, UseMvcc::Yes, region_count};

  dbgen_reset_seeds();

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
    const auto order = call_dbgen_mk<order_t>(order_idx + 1, mk_order, TpchTable::Orders, 0l, _scale_factor);

    order_builder.append_row(order.okey, order.custkey, std::string(1, order.orderstatus),
                             convert_money(order.totalprice), order.odate, order.opriority, order.clerk,
                             order.spriority, order.comment);

    for (auto line_idx = 0; line_idx < order.lines; ++line_idx) {
      const auto& lineitem = order.l[line_idx];

      lineitem_builder.append_row(lineitem.okey, lineitem.partkey, lineitem.suppkey, lineitem.lcnt, lineitem.quantity,
                                  convert_money(lineitem.eprice), convert_money(lineitem.discount),
                                  convert_money(lineitem.tax), std::string(1, lineitem.rflag[0]),
                                  std::string(1, lineitem.lstatus[0]), lineitem.sdate, lineitem.cdate, lineitem.rdate,
                                  lineitem.shipinstruct, lineitem.shipmode, lineitem.comment);
    }
  }

  /**
   * PART and PARTSUPP
   */

  for (size_t part_idx = 0; part_idx < part_count; ++part_idx) {
    const auto part = call_dbgen_mk<part_t>(part_idx + 1, mk_part, TpchTable::Part, _scale_factor);

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

  return {
      {TpchTable::Customer, customer_builder.finish_table()}, {TpchTable::Orders, order_builder.finish_table()},
      {TpchTable::LineItem, lineitem_builder.finish_table()}, {TpchTable::Part, part_builder.finish_table()},
      {TpchTable::PartSupp, partsupp_builder.finish_table()}, {TpchTable::Supplier, supplier_builder.finish_table()},
      {TpchTable::Nation, nation_builder.finish_table()},     {TpchTable::Region, region_builder.finish_table()}};
}

void TpchDbGenerator::generate_and_store() {
  const auto tables = generate();

  for (auto& table : tables) {
    StorageManager::get().add_table(tpch_table_names.at(table.first), table.second);
  }
}

}  // namespace opossum
