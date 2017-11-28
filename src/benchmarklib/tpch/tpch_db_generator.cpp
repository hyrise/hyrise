#include "tpch_db_generator.hpp"

extern "C" {
#include <dss.h>
#include <dsstypes.h>
#include <rnd.h>
}

#include <utility>

#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"

/**
 * Declare tpch_dbgen function we use that are not exposed by tpch-dbgen via headers
 */
extern "C" {
void row_start(int t);
void row_stop(int t);
}

extern char ** asc_date;
extern seed_t Seed[];

namespace {

// clang-format off
const auto customer_column_types = boost::hana::tuple      <int32_t,    std::string, std::string, int32_t,       std::string, float,       std::string,    std::string>();  // NOLINT
const auto customer_column_names = boost::hana::make_tuple("c_custkey", "c_name",    "c_address", "c_nationkey", "c_phone",   "c_acctbal", "c_mktsegment", "c_comment"); // NOLINT

const auto order_column_types = boost::hana::tuple      <int32_t,     int32_t,     std::string,     float,          std::string,   std::string,       std::string, int32_t,          std::string>();  // NOLINT
const auto order_column_names = boost::hana::make_tuple("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk",   "o_shippriority", "o_comment");  // NOLINT

const auto lineitem_column_types = boost::hana::tuple      <int32_t,     int32_t,     int32_t,     int32_t,        float,        float,             float,        float,   std::string,    std::string,    std::string,  std::string,    std::string,     std::string,      std::string,  std::string>();  // NOLINT
const auto lineitem_column_names = boost::hana::make_tuple("o_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment");  // NOLINT

const auto part_column_types = boost::hana::tuple      <int32_t,    std::string, std::string, std::string, std::string, int32_t,  std::string,   int32_t,        std::string>();  // NOLINT
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
template <typename... ColumnTypes>
class TableBuilder {
 public:
  template <typename... Strings>
  TableBuilder(size_t chunk_size, const boost::hana::tuple<ColumnTypes...>& column_types,
               const boost::hana::tuple<Strings...>& column_names) {
    _table = std::make_shared<opossum::Table>(chunk_size);

    const auto column_names_and_data_types = boost::hana::zip_with(
        [&](auto column_type, auto column_name) {
          return boost::hana::make_tuple(column_name, opossum::data_type_from_type<decltype(column_type)>());
        },
        column_types, column_names);

    boost::hana::fold_left(column_names_and_data_types, _table, [](auto table, auto column_name_and_type) {
      table->add_column_definition(column_name_and_type[boost::hana::llong_c<0>],
                                   column_name_and_type[boost::hana::llong_c<1>]);
      return table;
    });
  }

  std::shared_ptr<opossum::Table> finish_table() {
    if (_current_chunk_row_count() > 0) {
      _emit_chunk();
    }

    return _table;
  }

  void append_row(ColumnTypes&&... column_values) {
    auto vectors_and_values = boost::hana::zip_with(
        [](auto& vector, auto&& value) { return boost::hana::make_tuple(std::reference_wrapper(vector), value); },
        _column_vectors, boost::hana::make_tuple(std::forward<ColumnTypes>(column_values)...));

    boost::hana::for_each(vectors_and_values, [](auto vector_and_value) {
      vector_and_value[boost::hana::llong_c<0>].get().push_back(vector_and_value[boost::hana::llong_c<1>]);
    });

    if (_current_chunk_row_count() >= _table->max_chunk_size()) {
      _emit_chunk();
    }
  }

 private:
  std::shared_ptr<opossum::Table> _table;
  boost::hana::tuple<opossum::pmr_concurrent_vector<ColumnTypes>...> _column_vectors;

  size_t _current_chunk_row_count() const { return _column_vectors[boost::hana::llong_c<0>].size(); }

  void _emit_chunk() {
    opossum::Chunk chunk;

    boost::hana::for_each(_column_vectors, [&](auto&& vector) {
      using T = typename std::decay_t<decltype(vector)>::value_type;
      chunk.add_column(std::make_shared<opossum::ValueColumn<T>>(std::move(vector)));
      vector = typename std::decay_t<decltype(vector)>();
    });
    _table->emplace_chunk(std::move(chunk));
  }
};

std::unordered_map<opossum::TpchTable, std::underlying_type_t<opossum::TpchTable>> tpch_table_to_dbgen_id = {
    {opossum::TpchTable::Part, PART},     {opossum::TpchTable::PartSupp, PSUPP}, {opossum::TpchTable::Supplier, SUPP},
    {opossum::TpchTable::Customer, CUST}, {opossum::TpchTable::Orders, ORDER},   {opossum::TpchTable::LineItem, LINE},
    {opossum::TpchTable::Nation, NATION}, {opossum::TpchTable::Region, REGION}};

template <typename DSSType, typename MKRetType, typename... Args>
DSSType _call_dbgen_mk(size_t idx, MKRetType (*mk_fn)(DSS_HUGE, DSSType* val, Args...), opossum::TpchTable table,
                       Args... args) {
  /**
   * Preserve calling scheme (row_start(); mk...(); row_stop(); as in dbgen's gen_tbl())
   */

  const auto dbgen_table_id = tpch_table_to_dbgen_id.at(table);

  row_start(dbgen_table_id);

  DSSType value;
  mk_fn(idx, &value, std::forward<Args>(args)...);

  row_stop(dbgen_table_id);

  return value;
}

float _convert_money(DSS_HUGE cents) {
  const auto dollars = cents / 100;
  cents %= 100;
  return dollars + (static_cast<float>(cents)) / 100.0f;
}

void _free_and_null_permutations(distribution *d) {
  free(d->permute);
  d->permute = NULL;
}


/**
 * Call every time before using dbgen - initializes global state
 */
void _dbgen_setup() {
  Seed[0] = {PART, 1, 0, 1};
  Seed[0] = {PART, 1, 0, 1};
  Seed[0] = {PART, 1, 0, 1};


//
//  seed_t Seed[MAX_STREAM + 1] =
//  {
//  {PART,   1,          0,	1},					/* P_MFG_SD     0 */
//  {PART,   46831694,   0, 1},					/* P_BRND_SD    1 */
//  {PART,   1841581359, 0, 1},					/* P_TYPE_SD    2 */
//  {PART,   1193163244, 0, 1},					/* P_SIZE_SD    3 */
//  {PART,   727633698,  0, 1},					/* P_CNTR_SD    4 */
//  {NONE,   933588178,  0, 1},					/* text pregeneration  5 */
//  {PART,   804159733,  0, 2},	/* P_CMNT_SD    6 */
//  {PSUPP,  1671059989, 0, SUPP_PER_PART},     /* PS_QTY_SD    7 */
//  {PSUPP,  1051288424, 0, SUPP_PER_PART},     /* PS_SCST_SD   8 */
//  {PSUPP,  1961692154, 0, SUPP_PER_PART * 2},     /* PS_CMNT_SD   9 */
//  {ORDER,  1227283347, 0, 1},				    /* O_SUPP_SD    10 */
//  {ORDER,  1171034773, 0, 1},					/* O_CLRK_SD    11 */
//  {ORDER,  276090261,  0, 2},  /* O_CMNT_SD    12 */
//  {ORDER,  1066728069, 0, 1},					/* O_ODATE_SD   13 */
//  {LINE,   209208115,  0, O_LCNT_MAX},        /* L_QTY_SD     14 */
//  {LINE,   554590007,  0, O_LCNT_MAX},        /* L_DCNT_SD    15 */
//  {LINE,   721958466,  0, O_LCNT_MAX},        /* L_TAX_SD     16 */
//  {LINE,   1371272478, 0, O_LCNT_MAX},        /* L_SHIP_SD    17 */
//  {LINE,   675466456,  0, O_LCNT_MAX},        /* L_SMODE_SD   18 */
//  {LINE,   1808217256, 0, O_LCNT_MAX},      /* L_PKEY_SD    19 */
//  {LINE,   2095021727, 0, O_LCNT_MAX},      /* L_SKEY_SD    20 */
//  {LINE,   1769349045, 0, O_LCNT_MAX},      /* L_SDTE_SD    21 */
//  {LINE,   904914315,  0, O_LCNT_MAX},      /* L_CDTE_SD    22 */
//  {LINE,   373135028,  0, O_LCNT_MAX},      /* L_RDTE_SD    23 */
//  {LINE,   717419739,  0, O_LCNT_MAX},      /* L_RFLG_SD    24 */
//  {LINE,   1095462486, 0, O_LCNT_MAX * 2},   /* L_CMNT_SD    25 */
//  {CUST,   881155353,  0, 9},      /* C_ADDR_SD    26 */
//  {CUST,   1489529863, 0, 1},      /* C_NTRG_SD    27 */
//  {CUST,   1521138112, 0, 3},      /* C_PHNE_SD    28 */
//  {CUST,   298370230,  0, 1},      /* C_ABAL_SD    29 */
//  {CUST,   1140279430, 0, 1},      /* C_MSEG_SD    30 */
//  {CUST,   1335826707, 0, 2},     /* C_CMNT_SD    31 */
//  {SUPP,   706178559,  0, 9},      /* S_ADDR_SD    32 */
//  {SUPP,   110356601,  0, 1},      /* S_NTRG_SD    33 */
//  {SUPP,   884434366,  0, 3},      /* S_PHNE_SD    34 */
//  {SUPP,   962338209,  0, 1},      /* S_ABAL_SD    35 */
//  {SUPP,   1341315363, 0, 2},     /* S_CMNT_SD    36 */
//  {PART,   709314158,  0, 92},      /* P_NAME_SD    37 */
//  {ORDER,  591449447,  0, 1},      /* O_PRIO_SD    38 */
//  {LINE,   431918286,  0, 1},      /* HVAR_SD      39 */
//  {ORDER,  851767375,  0, 1},      /* O_CKEY_SD    40 */
//  {NATION, 606179079,  0, 2},      /* N_CMNT_SD    41 */
//  {REGION, 1500869201, 0, 2},      /* R_CMNT_SD    42 */
//  {ORDER,  1434868289, 0, 1},      /* O_LCNT_SD    43 */
//  {SUPP,   263032577,  0, 1},      /* BBB offset   44 */
//  {SUPP,   753643799,  0, 1},      /* BBB type     45 */
//  {SUPP,   202794285,  0, 1},      /* BBB comment  46 */
//  {SUPP,   715851524,  0, 1}       /* BBB junk     47 */
//  };
}

/**
 * Call this after using dbgen to avoid memory leaks
 */
void _dbgen_cleanup() {
  _free_and_null_permutations(&nations);
  _free_and_null_permutations(&regions);
  _free_and_null_permutations(&o_priority_set);
  _free_and_null_permutations(&l_instruct_set);
  _free_and_null_permutations(&l_smode_set);
  _free_and_null_permutations(&l_category_set);
  _free_and_null_permutations(&l_rflag_set);
  _free_and_null_permutations(&c_mseg_set);
  _free_and_null_permutations(&colors);
  _free_and_null_permutations(&p_types_set);
  _free_and_null_permutations(&p_cntr_set);
  _free_and_null_permutations(&articles);
  _free_and_null_permutations(&nouns);
  _free_and_null_permutations(&adjectives);
  _free_and_null_permutations(&adverbs);
  _free_and_null_permutations(&prepositions);
  _free_and_null_permutations(&verbs);
  _free_and_null_permutations(&terminators);
  _free_and_null_permutations(&auxillaries);
  _free_and_null_permutations(&np);
  _free_and_null_permutations(&vp);
  _free_and_null_permutations(&grammar);

  if (asc_date != NULL) {
    for (size_t idx = 0; idx < TOTDATE; ++idx) {
      free(asc_date[idx]);
    }
    free(asc_date);
  }
  asc_date = NULL;
}


}  // namespace

namespace opossum {

std::unordered_map<TpchTable, std::string> tpch_table_names = {
    {TpchTable::Part, "part"},         {TpchTable::PartSupp, "partsupp"}, {TpchTable::Supplier, "supplier"},
    {TpchTable::Customer, "customer"}, {TpchTable::Orders, "order"},      {TpchTable::LineItem, "lineitem"},
    {TpchTable::Nation, "nation"},     {TpchTable::Region, "region"}};

TpchDbGenerator::TpchDbGenerator(float scale_factor, uint32_t chunk_size)
    : _scale_factor(scale_factor), _chunk_size(chunk_size) {}

std::unordered_map<TpchTable, std::shared_ptr<Table>> TpchDbGenerator::generate() {
  TableBuilder customer_builder(_chunk_size, customer_column_types, customer_column_names);
  TableBuilder order_builder(_chunk_size, order_column_types, order_column_names);
  TableBuilder lineitem_builder(_chunk_size, lineitem_column_types, lineitem_column_names);
  TableBuilder part_builder(_chunk_size, part_column_types, part_column_names);
  TableBuilder partsupp_builder(_chunk_size, partsupp_column_types, partsupp_column_names);
  TableBuilder supplier_builder(_chunk_size, supplier_column_types, supplier_column_names);
  TableBuilder nation_builder(_chunk_size, nation_column_types, nation_column_names);
  TableBuilder region_builder(_chunk_size, region_column_types, region_column_names);

  _dbgen_setup();

  /**
   * CUSTOMER
   */
  const auto customer_count = static_cast<size_t>(tdefs[CUST].base * _scale_factor);

  for (size_t row_idx = 0; row_idx < customer_count; row_idx++) {
    auto customer = _call_dbgen_mk<customer_t>(row_idx + 1, mk_cust, TpchTable::Customer);
    customer_builder.append_row(customer.custkey, customer.name, customer.address, customer.nation_code, customer.phone,
                                _convert_money(customer.acctbal), customer.mktsegment, customer.comment);
  }

  /**
   * ORDER and LINEITEM
   */
  const auto order_count = static_cast<size_t>(tdefs[ORDER].base * _scale_factor);

  for (size_t order_idx = 0; order_idx < order_count; ++order_idx) {
    const auto order = _call_dbgen_mk<order_t>(order_idx + 1, mk_order, TpchTable::Orders, 0l, _scale_factor);

    order_builder.append_row(order.okey, order.custkey, std::string(1, order.orderstatus),
                             _convert_money(order.totalprice), order.odate, order.opriority, order.clerk,
                             order.spriority, order.comment);

    for (auto line_idx = 0; line_idx < order.lines; ++line_idx) {
      const auto& lineitem = order.l[line_idx];

      lineitem_builder.append_row(lineitem.okey, lineitem.partkey, lineitem.suppkey, lineitem.lcnt, lineitem.quantity,
                                  _convert_money(lineitem.eprice), _convert_money(lineitem.discount),
                                  _convert_money(lineitem.tax), std::string(1, lineitem.rflag[0]),
                                  std::string(1, lineitem.lstatus[0]), lineitem.sdate, lineitem.cdate, lineitem.rdate,
                                  lineitem.shipinstruct, lineitem.shipmode, lineitem.comment);
    }
  }

  /**
   * PART and PARTSUPP
   */
  const auto part_count = static_cast<size_t>(tdefs[PART].base * _scale_factor);

  for (size_t part_idx = 0; part_idx < part_count; ++part_idx) {
    const auto part = _call_dbgen_mk<part_t>(part_idx + 1, mk_part, TpchTable::Part, _scale_factor);

    part_builder.append_row(part.partkey, part.name, part.mfgr, part.brand, part.type, part.size, part.container,
                            _convert_money(part.retailprice), part.comment);

    for (const auto& partsupp : part.s) {
      partsupp_builder.append_row(partsupp.partkey, partsupp.suppkey, partsupp.qty, _convert_money(partsupp.scost),
                                  partsupp.comment);
    }
  }

  /**
   * SUPPLIER
   */
  const auto supplier_count = static_cast<size_t>(tdefs[SUPP].base * _scale_factor);

  for (size_t supplier_idx = 0; supplier_idx < supplier_count; ++supplier_idx) {
    const auto supplier = _call_dbgen_mk<supplier_t>(supplier_idx + 1, mk_supp, TpchTable::Supplier);

    supplier_builder.append_row(supplier.suppkey, supplier.name, supplier.address, supplier.nation_code, supplier.phone,
                                _convert_money(supplier.acctbal), supplier.comment);
  }

  /**
   * NATION
   */
  const auto nation_count = static_cast<size_t>(tdefs[NATION].base);

  for (size_t nation_idx = 0; nation_idx < nation_count; ++nation_idx) {
    const auto nation = _call_dbgen_mk<code_t>(nation_idx + 1, mk_nation, TpchTable::Nation);
    nation_builder.append_row(nation.code, nation.text, nation.join, nation.comment);
  }

  /**
   * REGION
   */
  const auto region_count = static_cast<size_t>(tdefs[REGION].base);

  for (size_t region_idx = 0; region_idx < region_count; ++region_idx) {
    const auto region = _call_dbgen_mk<code_t>(region_idx + 1, mk_region, TpchTable::Region);
    region_builder.append_row(region.code, region.text, region.comment);
  }

  /**
   * Clean up dbgen every time we finish table generation to avoid memory leaks in dbgen
   */
  _dbgen_cleanup();

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
