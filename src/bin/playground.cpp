#include <iostream>
#include <storage/chunk.hpp>
#include <tpcds/tpcds_table_generator.hpp>

#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  // TODO: reset
  auto generator = TpcdsTableGenerator(1, Chunk::DEFAULT_SIZE);
  generator.generate();
//  auto x = reinterpret_cast<int*>(malloc(5 * sizeof(int)));
//  std::cout << x[1] << '\n';
//  free(x);
}
