#include <iostream>

#include "operators/print.hpp"
#include "storage/table.hpp"
#include "tpch/tpch_db_generator.hpp"

extern "C" {
void mk_sparse(long i, long* ok, long seq);
}

int main() {
  opossum::TpchDbGenerator gen(0.001f, 10);

  auto tables = gen.generate();

  for (auto& pair : tables) {
    std::cout << "Table '" << pair.first << "'" << std::endl;
    opossum::Print::print(pair.second);
    std::cout << "\n";
  }

  return 0;
}
