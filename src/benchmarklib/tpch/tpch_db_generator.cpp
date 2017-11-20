#include "tpch_db_generator.hpp"

#include <utility>

//#include "dsstypes.h"

#include "storage/chunk.hpp"

namespace {

//enum TpchTable {
//    PART = 0,
//    PSUPP,
//    SUPP,
//    CUST,
//    ORDER,
//    LINE,
//    ORDER_LINE,
//    PART_PSUPP,
//    NATION,
//    REGION,
//    UPDATE,
//    MAX_TABLE,
//};

//void
//gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num)
//{
//  static order_t o;
//  supplier_t supp;
//  customer_t cust;
//  part_t part;
//  code_t code;
//  static int completed = 0;
//  DSS_HUGE i;
//
//  DSS_HUGE rows_per_segment=0;
//  DSS_HUGE rows_this_segment=-1;
//  DSS_HUGE residual_rows=0;
//
//  if (insert_segments)
//  {
//    rows_per_segment = count / insert_segments;
//    residual_rows = count - (rows_per_segment * insert_segments);
//  }
//
//  for (i = start; count; count--, i++)
//  {
//    LIFENOISE (1000, i);
//    row_start(tnum);
//
//    switch (tnum)
//    {
//      case LINE:
//      case ORDER:
//      case ORDER_LINE:
//        mk_order (i, &o, upd_num % 10000);
//
//        if (insert_segments  && (upd_num > 0))
//          if((upd_num / 10000) < residual_rows)
//          {
//            if((++rows_this_segment) > rows_per_segment)
//            {
//              rows_this_segment=0;
//              upd_num += 10000;
//            }
//          }
//          else
//          {
//            if((++rows_this_segment) >= rows_per_segment)
//            {
//              rows_this_segment=0;
//              upd_num += 10000;
//            }
//          }
//
//        if (set_seeds == 0)
//          tdefs[tnum].loader(&o, upd_num);
//        break;
//      case SUPP:
//        mk_supp (i, &supp);
//        if (set_seeds == 0)
//          tdefs[tnum].loader(&supp, upd_num);
//        break;
//      case CUST:
//        mk_cust (i, &cust);
//        if (set_seeds == 0)
//          tdefs[tnum].loader(&cust, upd_num);
//        break;
//      case PSUPP:
//      case PART:
//      case PART_PSUPP:
//        mk_part (i, &part);
//        if (set_seeds == 0)
//          tdefs[tnum].loader(&part, upd_num);
//        break;
//      case NATION:
//        mk_nation (i, &code);
//        if (set_seeds == 0)
//          tdefs[tnum].loader(&code, 0);
//        break;
//      case REGION:
//        mk_region (i, &code);
//        if (set_seeds == 0)
//          tdefs[tnum].loader(&code, 0);
//        break;
//    }
//    row_stop(tnum);
//    if (set_seeds && (i % tdefs[tnum].base) < 2)
//    {
//      printf("\nSeeds for %s at rowcount %ld\n", tdefs[tnum].comment, i);
//      dump_seeds(tnum);
//    }
//  }
//  completed |= 1 << tnum;
//}



}

namespace opossum {

TpchDbGenerator::TpchDbGenerator(float scale_factor, uint32_t chunk_size):
  _chunk_size(chunk_size)
{

}

std::unordered_map<std::string, std::shared_ptr<Table>> TpchDbGenerator::generate() {

//  for (i = PART; i <= REGION; i++)
//    if (table & (1 << i))
//    {
//      if (children > 1 && i < NATION)
//      {
//        partial ((int)i, step);
//      }
//      else
//      {
//        minrow = 1;
//        if (i < NATION)
//          rowcnt = tdefs[i].base * scale;
//        else
//          rowcnt = tdefs[i].base;
//        if (verbose > 0)
//          fprintf (stderr, "Generating data for %s", tdefs[i].comment);
//        gen_tbl ((int)i, minrow, rowcnt, upd_num);
//        if (verbose > 0)
//          fprintf (stderr, "done.\n");
//      }
//    }

  return {};
}

//void TpchDbGenerator::generate_and_store() {
//
//}
//
//void TpchDbGenerator::generate_and_export(const std::string &path) {
//
//}

//void TpchDbGenerator::_store_customer(const customer_t &customer) {
//  _c_custkeys.emplace_back(customer.custkey);
//  _c_names.emplace_back(customer.name);
//  _c_addresses.emplace_back(customer.address);
//  _c_nation_codes.emplace_back(customer.nation_code);
//  _c_phones.emplace_back(customer.phone);
//  _c_acctbals.emplace_back(customer.acctbal);
//  _c_mktsegments.emplace_back(customer.mktsegment);
//  _c_comments.emplace_back(customer.comment);
//
//  if (_chunk_size != 0 && _c_custkeys.size() >= _chunk_size) {
//    _emit_customer_chunk();
//  }
//}

//void TpchDbGenerator::_emit_customer_chunk() {
//  Chunk chunk;
//
//  _add_column_to_chunk(chunk, std::move(_c_custkeys));
//  _add_column_to_chunk(chunk, std::move(_c_names));
//  _add_column_to_chunk(chunk, std::move(_c_addresses));
//  _add_column_to_chunk(chunk, std::move(_c_nation_codes));
//  _add_column_to_chunk(chunk, std::move(_c_phones));
//  _add_column_to_chunk(chunk, std::move(_c_acctbals));
//  _add_column_to_chunk(chunk, std::move(_c_mktsegments));
//  _add_column_to_chunk(chunk, std::move(_c_comments));
//
//  _customer_table.emplace_chunk(std::move(chunk));
//}
//
//template<typename T>
//void TpchDbGenerator::_add_column_to_chunk(Chunk &chunk, std::vector<T> && data) {
//
//}

}