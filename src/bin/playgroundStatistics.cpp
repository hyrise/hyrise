#include <iostream>
#include <memory>

#include "../benchmark-libs/tpcc/tpcc_table_generator.hpp"
#include "all_type_variant.hpp"
#include "optimizer/statistics.hpp"
#include "storage/storage_manager.hpp"

int main() {
  std::cout << "starting main" << std::endl;

  tpcc::TableGenerator generator;

  std::cout << "starting generate table" << std::endl;

  opossum::StorageManager::get().add_table("CUSTOMER", generator.generate_customer_table());

  auto table_statistics = opossum::StorageManager::get().get_table("CUSTOMER")->table_statistics;
  std::cout << "stats: "
            << opossum::Statistics::predicate_stats(table_statistics, "C_ID", "=", opossum::AllTypeVariant(1))->row_count()
            << std::endl;
}
