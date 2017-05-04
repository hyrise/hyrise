#include <iostream>
#include <memory>
#include <utility>

#include "../benchmark/tpcc/tpcc_table_generator.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"

int main() {
  opossum::TPCCTableGenerator generator;

  auto item_table = generator.generate_items_table();
  auto warehouse_table = generator.generate_warehouse_table();
  auto stock_table = generator.generate_stock_table();
  auto district_table = generator.generate_district_table();
  auto customer_table = generator.generate_customer_table();

  opossum::StorageManager::get().add_table("ITEM", std::move(item_table));
  opossum::StorageManager::get().add_table("WAREHOUSE", std::move(warehouse_table));
  opossum::StorageManager::get().add_table("STOCK", std::move(stock_table));
  opossum::StorageManager::get().add_table("DISTRICT", std::move(district_table));
  opossum::StorageManager::get().add_table("CUSTOMER", std::move(customer_table));

  //  auto item = std::make_shared<opossum::GetTable>("ITEM");
  //  item->execute();
  //  auto print = std::make_shared<opossum::Print>(item);
  //  print->execute();
  //
  //  auto warehouse = std::make_shared<opossum::GetTable>("WAREHOUSE");
  //  warehouse->execute();
  //  auto print2 = std::make_shared<opossum::Print>(warehouse);
  //  print2->execute();
  //
  //  auto stock = std::make_shared<opossum::GetTable>("STOCK");
  //  stock->execute();
  //  auto print3 = std::make_shared<opossum::Print>(stock);
  //  print3->execute();
  //
  //  auto district = std::make_shared<opossum::GetTable>("DISTRICT");
  //  district->execute();
  //  auto print4 = std::make_shared<opossum::Print>(district);
  //  print4->execute();
  //
  //  auto customer = std::make_shared<opossum::GetTable>("CUSTOMER");
  //  customer->execute();
  //  auto print5 = std::make_shared<opossum::Print>(customer);
  //  print5->execute();
}
