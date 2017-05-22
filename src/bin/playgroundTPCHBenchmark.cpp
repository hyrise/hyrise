#include <iostream>
#include <memory>
#include <utility>

#include "../benchmark-libs/tpch/table_generator.hpp"
#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"

int main() {
  tpch::TableGenerator generator;

  generator.add_all_tables(opossum::StorageManager::get());

  // tables: SUPPLIER, PARTS
  auto item = std::make_shared<opossum::GetTable>("PARTS");
  item->execute();
  auto print = std::make_shared<opossum::Print>(item);
  print->execute();
}
