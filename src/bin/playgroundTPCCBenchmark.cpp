#include <iostream>
#include <memory>
#include <utility>

#include "../benchmark-libs/tpcc/table_generator.hpp"
#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"

int main() {
  tpcc::TableGenerator generator;

  generator.add_all_tables(opossum::StorageManager::get());

  // auto item = std::make_shared<opossum::GetTable>("ITEM");
  // item->execute();
  // auto print = std::make_shared<opossum::Print>(item);
  // print->execute();
  //
  // auto warehouse = std::make_shared<opossum::GetTable>("WAREHOUSE");
  // warehouse->execute();
  // auto print2 = std::make_shared<opossum::Print>(warehouse);
  // print2->execute();
  //
  // auto stock = std::make_shared<opossum::GetTable>("STOCK");
  // stock->execute();
  // auto print3 = std::make_shared<opossum::Print>(stock);
  // print3->execute();
  //
  // auto district = std::make_shared<opossum::GetTable>("DISTRICT");
  // district->execute();
  // auto print4 = std::make_shared<opossum::Print>(district);
  // print4->execute();
  //
  // auto customer = std::make_shared<opossum::GetTable>("CUSTOMER");
  // customer->execute();
  // auto print5 = std::make_shared<opossum::Print>(customer);
  // print5->execute();
  //
  // auto history = std::make_shared<opossum::GetTable>("HISTORY");
  // history->execute();
  // auto print6 = std::make_shared<opossum::Print>(history);
  // print6->execute();
  //
  // auto order = std::make_shared<opossum::GetTable>("ORDER");
  // order->execute();
  // auto print7 = std::make_shared<opossum::Print>(order);
  // print7->execute();
  //
  // auto order_line = std::make_shared<opossum::GetTable>("ORDER-LINE");
  // order_line->execute();
  // auto print8 = std::make_shared<opossum::Print>(order_line);
  // print8->execute();
  //
  // auto new_order = std::make_shared<opossum::GetTable>("NEW-ORDER");
  // new_order->execute();
  // auto print9 = std::make_shared<opossum::Print>(new_order);
  // print9->execute();
}
