#include "tpch_db_generator.hpp"

#include <utility>
#include <dsstypes.h>

extern "C" {
#include "dss.h"
#include <dsstypes.h>
#include <rnd.h>
}

#include "storage/chunk.hpp"

extern "C" {

void NthElement(DSS_HUGE N, DSS_HUGE *StartSeed);
DSS_HUGE set_state(int table, long sf, long procs, long step, DSS_HUGE *extra_rows);
void load_dists(void);

}

namespace opossum {

TpchDbGenerator::TpchDbGenerator(float scale_factor, uint32_t chunk_size) :
_scale_factor(scale_factor),
_chunk_size(chunk_size) {
  load_dists();

//  tdefs[ORDER].base *=
//  ORDERS_PER_CUST;			/* have to do this after init */
//  tdefs[LINE].base *=
//  ORDERS_PER_CUST;			/* have to do this after init */
//  tdefs[ORDER_LINE].base *=
//  ORDERS_PER_CUST;			/* have to do this after init */
}

std::unordered_map<std::string, std::shared_ptr<Table>> TpchDbGenerator::generate() {
  /**
   * CUSTOMER
   */
  TableBuilder<int64_t, std::string,
  std::string,
  int64_t,
  std::string,
  int64_t,
  std::string,
  std::string
  > customer_builder(_chunk_size,
                     boost::hana::make_tuple("c_custkey", "c_name", "c_address", "c_nation_code", "c_phone",
                                             "c_acctbal", "c_mktsegment", "c_comment"));

  auto customer_count = static_cast<size_t>(tdefs[TpchTable_Customer].base * _scale_factor);

  for (size_t row_idx = 1; row_idx <= customer_count; row_idx++) {
    _row_start();

    customer_t customer;
    mk_cust(row_idx, &customer);
    customer_builder.append_row(
    customer.custkey, customer.name, customer.address, customer.nation_code, customer.phone, customer.acctbal,
    customer.mktsegment, customer.comment
    );

    row_stop(table);
  }

  /**
   * ORDER and LINEITEM
   */
  TableBuilder<int64_t, int64_t, std::string, float, std::string, std::string, std::string, int32_t, std::string>
  order_builder(_chunk_size,
                boost::hana::make_tuple("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
                                        "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"));

  TableBuilder<int32_t, int32_t, int32_t, int32_t, float, float, float, float, std::string, std::string, std::string, std::string, std::string, std::string, std::string, std::string>
    lineitem_builder(_chunk_size, "orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
                     "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate",
                     "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment")

  auto order_count = static_cast<size_t>(tdefs[TpchTable_Order].base * _scale_factor);

//  DSS_HUGE rows_per_segment=0;
//  DSS_HUGE rows_this_segment=-1;
//
//  for (size_t order_idx = 0; order_idx < order_count; ++order_idx) {
//    order_t order;
//    mk_order(order_idx, &order, update_num);
//
//    if((++rows_this_segment) >= rows_per_segment)
//    {
//      rows_this_segment=0;
//      upd_num += 10000;
//    }
//
//    order_builder.append_row(order.okey, order.custkey, order.orderstatus, order.totalprice, order.odate,
//                             order.opriority, order.clerk, order.spriority, o->comment);
//
//    for (auto line_idx = 0; line_idx < order.lines; ++line_idx) {
//      const auto& lineitem = order.l[line_idx];
//
//      lineitem_builder.append_row(lineitem.okey, lineitem.partkey, lineitem.suppkey, lineitem.lcnt, lineitem.quantity,
//        lineitem.eprice, lineitem.discount, lineitem.tax, lineitem.rflag[0], lineitem.lstatus[0], lineitem.sdate,
//        lineitem.cdate, lineitem.rdate, lineitem.shipinstruct, lineitem.shipmode, lineitem.comment);
//    }
//  }
  
  return {
  {"customer", customer_builder.finish_table()},
  {"order", order_builder.finish_table()},
  {"lineitem_builder", lineitem_builder.finish_table()}
  };

}

//void TpchDbGenerator::generate_and_store() {
//
//}
//
//void TpchDbGenerator::generate_and_export(const std::string &path) {
//
//}

std::shared_ptr<Table> TpchDbGenerator::_generate_customer_table() {


}

void
TpchDbGenerator::_row_start() {
  for (int i = 0; i <= MAX_STREAM; i++)
    Seed[i].usage = 0;
}

void
TpchDbGenerator::_row_stop(TpchTable table) {
  int i;

  /* need to allow for handling the master and detail together */
  if (table == TpchTable_OrderLine) table = TpchTable_Order;
  if (table == TpchTable_PartSupplier) table = TpchTable_Part;

  for (i = 0; i <= MAX_STREAM; i++)
    if ((Seed[i].table == table) ", "", " (Seed[i].table == tdefs[table].child)) {
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

}