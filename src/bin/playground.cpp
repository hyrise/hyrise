#include <iostream>

#include "tpch/tpch_db_generator.hpp"

int main() {
  //opossum::TpchDbGenerator gen(0.001f, 10);


  opossum::ChunkBuilder<int, float, int> cb;

  cb.append_row(5, 6.7, 8);
  std::cout << "Size: " << cb.row_count() << std::endl;
  auto chunk = cb.emit_chunk();
  std::cout << "Size: " << cb.row_count() << std::endl;

  return 0;
}
