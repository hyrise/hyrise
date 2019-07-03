#include <iostream>

#include "storage/storage_manager.hpp"
#include "tpcds/tpcds_table_generator.hpp"

int main() {
//  std::cout << "TPCDS" << std::endl;
//  std::cout << " > Generating tables" << std::endl;

//  opossum::TpcdsTableGenerator(1, 1000).generate_and_store();

//  std::cout << " > Dumping as CSV" << std::endl;
//  opossum::StorageManager::get().export_all_tables_as_csv(".");
//
//  std::cout << " > Done" << std::endl;
//
//  return 0;

  opossum::TpcdsTableGenerator(10, 1000).generate();
  // TODO
}
