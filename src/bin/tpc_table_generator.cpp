#include <iostream>
#include <memory>

#include "operators/export_csv.hpp"
#include "operators/table_wrapper.hpp"

#include "tpcc/tpcc_table_generator.hpp"

int main() {
  std::cout << "TPCC" << std::endl;
  std::cout << " > Generating tables" << std::endl;
  auto tables = tpcc::TableGenerator().generate_all_tables();

  for (auto& pair : *tables) {
    opossum::StorageManager::get().add_table(pair.first, pair.second);
  }

  std::cout << " > Dumping as CSV" << std::endl;
  opossum::StorageManager::get().dump_as_csv(".");

  std::cout << " > Done" << std::endl;

  return 0;
}
