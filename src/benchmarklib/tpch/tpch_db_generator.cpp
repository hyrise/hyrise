#include "tpch_db_generator.hpp"

#include <utility>

extern "C" {
#include <dss.h>
#include <dsstypes.h>
#include <rnd.h>
}

#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"

extern "C" {

void NthElement(DSS_HUGE N, DSS_HUGE* StartSeed);
DSS_HUGE set_state(int table, long sf, long procs, long step, DSS_HUGE* extra_rows);
}

namespace {

template <typename... ColumnTypes>
class TableBuilder {
 public:
  template <typename... Strings>
  TableBuilder(size_t chunk_size, boost::hana::tuple<Strings...> column_names) {
    _table = std::make_shared<opossum::Table>(chunk_size);

    boost::hana::zip_with(
      [&](auto column_type, auto column_name) {
        _table->add_column_definition(column_name, opossum::data_type_from_type<decltype(column_type)>());
        return 0;
      },
      boost::hana::tuple<ColumnTypes...>(), column_names);
  }

  std::shared_ptr<opossum::Table> finish_table() {
    if (_current_chunk_row_count() > 0) {
      _emit_chunk();
    }

    return _table;
  }

  void append_row(ColumnTypes&&... column_values) {
    boost::hana::zip_with(
      [](auto& vector, auto&& value) {
        vector.push_back(value);
        return 0;
      },
      _column_vectors, boost::hana::make_tuple(std::forward<ColumnTypes>(column_values)...));

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
    {opossum::TpchTable::Part, PART},     {opossum::TpchTable::PartSupp, PSUPP},
    {opossum::TpchTable::Supplier, SUPP}, {opossum::TpchTable::Customer, CUST},
    {opossum::TpchTable::Orders, ORDER},   {opossum::TpchTable::LineItem, LINE},
    {opossum::TpchTable::Nation, NATION}, {opossum::TpchTable::Region, REGION}};
}

namespace opossum {

std::unordered_map<TpchTable, std::string> tpch_table_names = {
  {TpchTable::Part,     "part"},
  {TpchTable::PartSupp, "partsupp"},
  {TpchTable::Supplier, "supplier"},
  {TpchTable::Customer, "customer"},
  {TpchTable::Orders,   "order"},
  {TpchTable::LineItem, "lineitem"},
  {TpchTable::Nation,   "nation"},
  {TpchTable::Region,   "region"}};

TpchDbGenerator::TpchDbGenerator(float scale_factor, uint32_t chunk_size)
  : _scale_factor(scale_factor), _chunk_size(chunk_size) {}

std::unordered_map<TpchTable, std::shared_ptr<Table>> TpchDbGenerator::generate() {
  /**
   * CUSTOMER
   */
  TableBuilder<int64_t, std::string, std::string, int64_t, std::string, int64_t, std::string, std::string>
    customer_builder(_chunk_size, boost::hana::make_tuple("c_custkey", "c_name", "c_address", "c_nation_code",
                                                          "c_phone", "c_acctbal", "c_mktsegment", "c_comment"));

  const auto customer_count = static_cast<size_t>(tdefs[CUST].base * _scale_factor);

  for (size_t row_idx = 1; row_idx <= customer_count; row_idx++) {
    auto customer = _call_dbgen_mk<customer_t>(row_idx + 1, mk_cust, TpchTable::Customer);
    customer_builder.append_row(customer.custkey, customer.name, customer.address, customer.nation_code, customer.phone,
                                customer.acctbal, customer.mktsegment, customer.comment);
  }

  /**
   * ORDER and LINEITEM
   */
  TableBuilder<int64_t, int64_t, std::string, float, std::string, std::string, std::string, int32_t, std::string>
    order_builder(_chunk_size,
                  boost::hana::make_tuple("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
                                          "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"));

  TableBuilder<int32_t, int32_t, int32_t, int32_t, float, float, float, float, std::string, std::string, std::string,
    std::string, std::string, std::string, std::string, std::string>
    lineitem_builder(_chunk_size,
                     boost::hana::make_tuple("o_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
                                             "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
                                             "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct",
                                             "l_shipmode", "l_comment"));

  const auto order_count = static_cast<size_t>(tdefs[ORDER].base * _scale_factor);

  for (size_t order_idx = 0; order_idx < order_count; ++order_idx) {
    order_t order;
    mk_order(order_idx + 1, &order, 0, _scale_factor);

    order_builder.append_row(order.okey, order.custkey, std::string(1, order.orderstatus), order.totalprice,
                             order.odate, order.opriority, order.clerk, order.spriority, order.comment);

    for (auto line_idx = 0; line_idx < order.lines; ++line_idx) {
      const auto &lineitem = order.l[line_idx];

      lineitem_builder.append_row(lineitem.okey, lineitem.partkey, lineitem.suppkey, lineitem.lcnt, lineitem.quantity,
                                  lineitem.eprice, lineitem.discount, lineitem.tax, std::string(1, lineitem.rflag[0]),
                                  std::string(1, lineitem.lstatus[0]), lineitem.sdate, lineitem.cdate, lineitem.rdate,
                                  lineitem.shipinstruct, lineitem.shipmode, lineitem.comment);
    }
  }

  /**
   * PART and PARTSUPP
   */
  TableBuilder<int32_t, std::string, std::string, std::string, std::string, int32_t, std::string, int32_t, std::string>
    part_builder(_chunk_size, boost::hana::make_tuple("p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size",
                                                      "p_container", "p_retailsize", "p_comment"));

  TableBuilder<int32_t, int32_t, int32_t, float, std::string> partsupp_builder(
    _chunk_size, boost::hana::make_tuple("ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"));

  const auto part_count = static_cast<size_t>(tdefs[PART].base * _scale_factor);

  for (size_t part_idx = 0; part_idx < part_count; ++part_idx) {
    part_t part;
    mk_part(part_idx + 1, &part, _scale_factor);

    part_builder.append_row(part.partkey, part.name, part.mfgr, part.brand, part.type, part.size, part.container,
                            part.retailprice, part.comment);

    for (size_t partsupp_idx = 0; partsupp_idx < SUPP_PER_PART; ++partsupp_idx) {
      auto &partsupp = part.s[partsupp_idx];
      partsupp_builder.append_row(partsupp.partkey, partsupp.suppkey, partsupp.qty, partsupp.scost, partsupp.comment);
    }
  }

  /**
   * SUPPLIER
   */
  TableBuilder<int32_t, std::string, std::string, int32_t, std::string, float, std::string> supplier_builder(
    _chunk_size,
    boost::hana::make_tuple("s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"));

  const auto supplier_count = static_cast<size_t>(tdefs[SUPP].base * _scale_factor);

  for (size_t supplier_idx = 0; supplier_idx < supplier_count; ++supplier_idx) {
    supplier_t supplier;
    mk_supp(supplier_idx + 1, &supplier);

    supplier_builder.append_row(supplier.suppkey, supplier.name, supplier.address, supplier.nation_code, supplier.phone,
                                supplier.acctbal, supplier.comment);
  }

  /**
   * NATION
   */
  TableBuilder<int32_t, std::string, int32_t, std::string> nation_builder(
    _chunk_size, boost::hana::make_tuple("n_nationkey", "n_name", "n_regionkey", "n_comment"));

  const auto nation_count = static_cast<size_t>(tdefs[NATION].base);

  for (size_t nation_idx = 0; nation_idx < nation_count; ++nation_idx) {
    code_t nation;
    mk_nation(nation_idx + 1, &nation);

    nation_builder.append_row(nation.code, nation.text, nation.join, nation.comment);
  }

  /**
   * REGION
   */
  TableBuilder<int32_t, std::string, std::string> region_builder(
    _chunk_size, boost::hana::make_tuple("r_regionkey", "r_name", "r_comment"));

  const auto region_count = static_cast<size_t>(tdefs[REGION].base);

  for (size_t region_idx = 0; region_idx < region_count; ++region_idx) {
    code_t region;
    mk_region(region_idx + 1, &region);

    region_builder.append_row(region.code, region.text, region.comment);
  }

  /**
   * Clean up dbgen
   */
  dbgen_cleanup();

  return {{TpchTable::Customer, customer_builder.finish_table()},
          {TpchTable::Orders,   order_builder.finish_table()},
          {TpchTable::LineItem, lineitem_builder.finish_table()},
          {TpchTable::Part,     part_builder.finish_table()},
          {TpchTable::PartSupp, partsupp_builder.finish_table()},
          {TpchTable::Supplier, supplier_builder.finish_table()},
          {TpchTable::Nation,   nation_builder.finish_table()},
          {TpchTable::Region,   region_builder.finish_table()}};
}

void TpchDbGenerator::generate_and_store() {
  const auto tables = generate();

  for (auto &table : tables) {
    StorageManager::get().add_table(tpch_table_names.at(table.first), table.second);
  }
}

template<typename T, typename ...Args>
T TpchDbGenerator::_call_dbgen_mk(size_t idx, long (*mk_fn)(long long int, T *val, Args...), TpchTable table, Args ... args) const {
  // dbgen's row_start()
  for (int i = 0; i <= MAX_STREAM; i++) Seed[i].usage = 0;

  /**
   * Call mk_*
   */
  T value;
  mk_fn(idx, &value, std::forward<Args>(args)...);

  /**
   * dbgen's row_stop()
   */
  int i;

  for (i = 0; i <= MAX_STREAM; i++) {
    const auto dbgen_table_id = tpch_table_to_dbgen_id.at(table);
    if ((Seed[i].table == dbgen_table_id) || (Seed[i].table == tdefs[dbgen_table_id].child)) {
      if (Seed[i].usage > Seed[i].boundary) {
        Seed[i].boundary = Seed[i].usage;
      } else {
        NthElement((Seed[i].boundary - Seed[i].usage), &Seed[i].value);
#ifdef RNG_TEST
        Seed[i].nCalls += Seed[i].boundary - Seed[i].usage;
#endif
      }
    }
  }

  return value;
}
}  // namespace opossum