#include <iostream>
#include <memory>
#include <utility>

#include "../benchmark-libs/tpch/tpch_table_generator.hpp"
#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"

int main() {
  tpch::TableGenerator generator;

  generator.add_all_tables(opossum::StorageManager::get());

  // tables: SUPPLIER, PART, PARTSUPP, CUSTOMER, ORDERS, LINEITEM, NATION
  auto table1 = std::make_shared<opossum::GetTable>("NATION");
  table1->execute();
  auto print1 = std::make_shared<opossum::Print>(table1);
  print1->execute();
  // auto table2 = std::make_shared<opossum::GetTable>("LINEITEM");
  // table2->execute();
  // auto print2 = std::make_shared<opossum::Print>(table2);
  // print2->execute();
}
