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
  auto stat1 = table_statistics->predicate_statistics("C_ID", "=", opossum::AllParameterVariant(1));
  auto stat2 = stat1->predicate_statistics("C_D_ID", "!=", opossum::AllParameterVariant(2));
  auto stat3 = stat1->predicate_statistics("C_D_ID", "<", opossum::AllParameterVariant(5));
  std::cout << "original CUSTOMER table" << std::endl;
  std::cout << *table_statistics << std::endl;
  std::cout << "filtered (C_ID = 1)" << std::endl;
  std::cout << *stat1 << std::endl;
  std::cout << "filtered (C_D_ID != 2)" << std::endl;
  std::cout << *stat2 << std::endl;
  std::cout << "filtered (C_D_ID < 5)" << std::endl;
  std::cout << *stat3 << std::endl;
}
