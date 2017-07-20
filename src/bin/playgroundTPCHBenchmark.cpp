#include <iostream>
#include <memory>
#include <utility>

#include "../benchmark-libs/tpch/tpch_table_generator.hpp"
#include "../benchmark-libs/tpcc/tpcc_table_generator.hpp"
#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"

int main() {
  std::cout << "TPCH" << std::endl;
  std::cout << " > Generating tables" << std::endl;
  auto tables = tpch::TableGenerator().generate_all_tables();
  std::cout << " > Adding tables to StorageManager" << std::endl;

  for (auto& pair : *tables) {
    opossum::StorageManager::get().add_table(pair.first, pair.second);
  }
  std::cout << " > Printing Contents" << std::endl;
  // tables: SUPPLIER, PART, PARTSUPP, CUSTOMER, ORDERS, LINEITEM, NATION, REGION
  // auto table1 = std::make_shared<opossum::GetTable>("LINEITEM");
  // table1->execute();
  // auto print1 = std::make_shared<opossum::Print>(table1);
  // print1->execute();
  // auto table2 = std::make_shared<opossum::GetTable>("LINEITEM");
  // table2->execute();
  // auto print2 = std::make_shared<opossum::Print>(table2);
  // print2->execute();
  std::cout << "TPCC" << std::endl;
  std::cout << " > Generating tables" << std::endl;
  tables = tpcc::TableGenerator().generate_all_tables();

  for (auto& pair : *tables) {
    opossum::StorageManager::get().add_table(pair.first, pair.second);
  }

  auto table2 = std::make_shared<opossum::GetTable>("ORDER-LINE");
  table2->execute();
  auto print2 = std::make_shared<opossum::Print>(table2);
  print2->execute();
}
