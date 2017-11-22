#include <iostream>
#include <limits>

#include "types.hpp"

int main() {
  std::cout << std::numeric_limits<opossum::ChunkID>::min() << std::endl;
  std::cout << std::numeric_limits<opossum::ChunkID>::max() << std::endl;
  return 0;
}
