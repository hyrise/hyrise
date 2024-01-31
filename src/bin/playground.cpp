#include <iostream>

#include "types.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  std::cout << std::alignment_of<RowID>::value << ' ';
  std::cout << std::alignment_of<std::pair<RowID, bool>>::value << ' ';
  std::cout << std::alignment_of<std::tuple<ChunkID, ChunkOffset, bool>>::value << ' ';

  std::cout << sizeof(RowID) << ' ';
  std::cout << sizeof(std::pair<RowID, bool>) << ' ';
  std::cout << sizeof(std::tuple<ChunkID, ChunkOffset, bool>) << ' ';
  return 0;
}
