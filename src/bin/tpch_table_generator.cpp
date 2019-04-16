#include <iostream>

#include "storage/storage_manager.hpp"
#include "tpch/tpch_table_generator.hpp"

#include "utils/assert.hpp"

int main(int argc, char** argv) {
  Assert(argc == 2, "Usage: tpchTableGenerator <scale_factor>");

  const auto scale_factor = std::stof(argv[1]);

  std::cout << "TPCH" << std::endl;
  std::cout << " > Generating tables" << std::endl;
  opossum::TpchTableGenerator(scale_factor, 1000).generate_and_store();

  std::cout << " > Dumping as CSV" << std::endl;
  opossum::StorageManager::get().export_all_tables_as_csv(".");

  std::cout << " > Done" << std::endl;

  return 0;
}
