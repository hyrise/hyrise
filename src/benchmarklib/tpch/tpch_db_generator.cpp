#include "tpch_db_generator.hpp"

#include <utility>

extern "C" {
#include "dss.h"
#include <dsstypes.h>
#include <rnd.h>
}

#include "storage/chunk.hpp"

extern "C" {

void NthElement(DSS_HUGE N, DSS_HUGE *StartSeed);
DSS_HUGE set_state(int table, long sf, long procs, long step, DSS_HUGE *extra_rows);
void load_dists (void);

}

namespace opossum {

TpchDbGenerator::TpchDbGenerator(float scale_factor, uint32_t chunk_size):
  _scale_factor(scale_factor),
  _chunk_size(chunk_size)
{
  load_dists();

//  tdefs[ORDER].base *=
//  ORDERS_PER_CUST;			/* have to do this after init */
//  tdefs[LINE].base *=
//  ORDERS_PER_CUST;			/* have to do this after init */
//  tdefs[ORDER_LINE].base *=
//  ORDERS_PER_CUST;			/* have to do this after init */
}

std::unordered_map<std::string, std::shared_ptr<Table>> TpchDbGenerator::generate() {
  std::unordered_map<std::string, std::shared_ptr<Table>> tables_by_name;

  tables_by_name["customer"] = _generate_customer_table();

  return tables_by_name;
}

//void TpchDbGenerator::generate_and_store() {
//
//}
//
//void TpchDbGenerator::generate_and_export(const std::string &path) {
//
//}

std::shared_ptr<Table> TpchDbGenerator::_generate_customer_table() {
  TableBuilder<
  int64_t,
  std::string,
  std::string,
  int64_t,
  std::string,
  int64_t,
  std::string,
  std::string
  > customer_builder(_chunk_size, boost::hana::make_tuple("c_custkey", "c_name", "c_address", "c_nation_code", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"));

  size_t row_count = static_cast<size_t>(tdefs[TpchTable_Customer].base * _scale_factor);

  for (size_t row_idx = 1; row_idx <= row_count; row_idx++) {
    _row_start();

    customer_t customer;
    mk_cust (row_idx, &customer);
    customer_builder.append_row(
      customer.custkey, customer.name, customer.address, customer.nation_code, customer.phone, customer.acctbal,
      customer.mktsegment, customer.comment
    );

    row_stop(table);
  }

  return customer_builder.finish_table();
}

void
TpchDbGenerator::_row_start()
{
  for (int i=0; i <= MAX_STREAM; i++)
    Seed[i].usage = 0 ;
}

void
TpchDbGenerator::_row_stop(TpchTable table)
	{
  int i;

  /* need to allow for handling the master and detail together */
  if (table == TpchTable_OrderLine) table = TpchTable_Order;
  if (table == TpchTable_PartSupplier) table = TpchTable_Part;

  for (i=0; i <= MAX_STREAM; i++)
    if ((Seed[i].table == table) || (Seed[i].table == tdefs[table].child))
    {
      if (Seed[i].usage > Seed[i].boundary)
      {
        Seed[i].boundary = Seed[i].usage;
      }
      else
      {
        NthElement((Seed[i].boundary - Seed[i].usage), &Seed[i].value);
#ifdef RNG_TEST
        Seed[i].nCalls += Seed[i].boundary - Seed[i].usage;
#endif
      }
    }
}

}