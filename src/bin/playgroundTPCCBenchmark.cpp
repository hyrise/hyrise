#include <iostream>
#include <memory>
#include <utility>

#include "../benchmark/tpcc/tpcc_table_generator.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"

int main() {
  std::cout << "starting tpcc playground" << std::endl;
  opossum::TPCCTableGenerator generator;

  auto item_table = generator.generate_items_table();

  std::cout << "items table generated" << std::endl;

  opossum::StorageManager::get().add_table("ITEM", std::move(item_table));

  auto item = std::make_shared<opossum::GetTable>("ITEM");
  item->execute();

  std::cout << "get table executed" << std::endl;

  auto print = std::make_shared<opossum::Print>(item);
  print->execute();

  auto result = item->get_output();
  auto row_count = result->row_count();
  std::cout << row_count << std::endl;
}
