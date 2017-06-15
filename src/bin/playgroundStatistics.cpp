#include <iostream>
#include <memory>

#include "../benchmark-libs/tpcc/tpcc_table_generator.hpp"
#include "all_parameter_variant.hpp"
#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"

int main() {
  std::cout << "starting main" << std::endl;

  tpcc::TableGenerator generator;

  std::cout << "starting generate table" << std::endl;

  opossum::StorageManager::get().add_table("CUSTOMER", generator.generate_customer_table());

  auto table_statistics = opossum::StorageManager::get().get_table("CUSTOMER")->table_statistics;
  std::cout << "stats: "
            << table_statistics->predicate_statistics("C_ID", "=", opossum::AllParameterVariant(1))->row_count()
            << std::endl;
}
