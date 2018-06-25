#include <iostream>

#include "types.hpp"

#include "table_generator.hpp"
#include "operators/print.hpp"

using namespace opossum;  // NOLINT

int main() {
  TableGenerator generator;
  generator.num_columns = 2;
  generator.num_rows = 50;

  const auto table = generator.generate_table(13, 0.2f);
  Print::print(table);

  return 0;
}
