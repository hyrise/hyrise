#include <iostream>
#include <memory>

#include "../benchmark-libs/tpcc/tpcc_table_generator.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"

int main() {
  std::cout << "starting main" << std::endl;

  tpcc::TableGenerator generator;

  std::cout << "starting generate all tables" << std::endl;

  auto tables = generator.generate_all_tables();

  std::cout << "finished generate all tables" << std::endl;

  opossum::StorageManager::get().add_table("HISTORY", tables->at("HISTORY"));
  opossum::StorageManager::get().add_table("ORDER-LINE", tables->at("ORDER-LINE"));
  opossum::StorageManager::get().add_table("ITEM", tables->at("ITEM"));
  opossum::StorageManager::get().add_table("WAREHOUSE", tables->at("WAREHOUSE"));
  opossum::StorageManager::get().add_table("STOCK", tables->at("STOCK"));
  opossum::StorageManager::get().add_table("DISTRICT", tables->at("DISTRICT"));
  opossum::StorageManager::get().add_table("CUSTOMER", tables->at("CUSTOMER"));
  opossum::StorageManager::get().add_table("ORDER", tables->at("ORDER"));
  opossum::StorageManager::get().add_table("NEW-ORDER", tables->at("NEW-ORDER"));

  std::cout << "finished storage manager add" << std::endl;

  //   auto item = std::make_shared<opossum::GetTable>("ITEM");
  //   item->execute();
  //   auto print = std::make_shared<opossum::Print>(item);
  //   print->execute();
  //
  //   auto warehouse = std::make_shared<opossum::GetTable>("WAREHOUSE");
  //   warehouse->execute();
  //   auto print2 = std::make_shared<opossum::Print>(warehouse);
  //   print2->execute();
  //
  //   auto stock = std::make_shared<opossum::GetTable>("STOCK");
  //   stock->execute();
  //   auto print3 = std::make_shared<opossum::Print>(stock);
  //   print3->execute();
  //
  //   auto district = std::make_shared<opossum::GetTable>("DISTRICT");
  //   district->execute();
  //   auto print4 = std::make_shared<opossum::Print>(district);
  //   print4->execute();
  //
  //   auto customer = std::make_shared<opossum::GetTable>("CUSTOMER");
  //   customer->execute();
  //   auto print5 = std::make_shared<opossum::Print>(customer);
  //   print5->execute();
  //
  //    auto history = std::make_shared<opossum::GetTable>("HISTORY");
  //    history->execute();
  //    auto print6 = std::make_shared<opossum::Print>(history);
  //    print6->execute();
  //
  //   auto order = std::make_shared<opossum::GetTable>("ORDER");
  //   order->execute();
  //   auto print7 = std::make_shared<opossum::Print>(order);
  //   print7->execute();

  auto order_line = std::make_shared<opossum::GetTable>("ORDER-LINE");
  order_line->execute();
  std::cout << "finished execute get table orderline" << std::endl;

  //  auto print8 = std::make_shared<opossum::Print>(order_line);
  //  print8->execute();

  std::cout << "finished everything" << std::endl;

  //   auto new_order = std::make_shared<opossum::GetTable>("NEW-ORDER");
  //   new_order->execute();
  //   auto print9 = std::make_shared<opossum::Print>(new_order);
  //   print9->execute();

  return 0;
}
