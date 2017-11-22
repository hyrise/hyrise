#include <iostream>

#include "operators/print.hpp"
#include "storage/table.hpp"
#include "tpch/tpch_db_generator.hpp"

int main() {
//  opossum::TableBuilder<
//  int64_t,
//  std::string,
//  std::string,
//  int64_t,
//  std::string,
//  int64_t,
//  std::string,
//  std::string
//  > customer_builder(0, boost::hana::make_tuple("c_custkey", "c_name", "c_address", "c_nation_code", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"));
//
//  customer_builder.finish_table();


  opossum::TpchDbGenerator gen(0.001f, 10);

  auto tables = gen.generate();

  for (auto & pair : tables) {
    std::cout << "Table '" << pair.first << "'" << std::endl;
    opossum::Print::print(pair.second);
    std::cout << "\n";
  }

  return 0;
}
