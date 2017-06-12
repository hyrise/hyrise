#include <iostream>
#include <memory>

#include "all_type_variant.hpp"
#include "../benchmark-libs/tpcc/tpcc_table_generator.hpp"
#include "optimizer/statistics.hpp"

int main() {
  std::cout << "starting main" << std::endl;

  tpcc::TableGenerator generator;

  std::cout << "starting generate table" << std::endl;

  auto customer_table = generator.generate_customer_table();

  std::cout << "stats: " << opossum::Statistics::predicate_result_size(customer_table, "C_ID", "=", opossum::AllTypeVariant(1)) << std::endl;
}
