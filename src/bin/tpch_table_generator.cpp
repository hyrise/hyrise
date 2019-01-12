#include <iostream>

#include "storage/storage_manager.hpp"
#include "tpch/tpch_table_generator.hpp"

int main() {
  std::cout << "TPCH" << std::endl;
  std::cout << " > Generating tables" << std::endl;
  opossum::TpchTableGenerator(1.0f, 1000).generate_and_store();

  std::cout << " > Dumping as CSV" << std::endl;
  opossum::StorageManager::get().export_all_tables_as_csv(".");

  std::cout << " > Done" << std::endl;

  return 0;
}
