#include <iostream>
#include <bitset>

#include "types.hpp"

using namespace opossum;  // NOLINT

template <typename T>
void hashish(T value) {
	size_t hash = std::hash<T>{}(value);
	size_t new_hash = (std::hash<T>{}(value) * 11400714819323198485llu) >> 32;
	std::cout << value << " >> " << std::bitset<64>(hash) << " -- " << std::bitset<64>(new_hash) << std::endl;
}

int main() {
  hashish(1.2f);
  hashish(12.0f);
  hashish(120.0f);
  hashish(1200000000.0f);

  hashish(1.2);
  hashish(12.0);
  hashish(120.0);
  hashish(1200000000.0);

  hashish(1);
  hashish(12);
  hashish(120);
  hashish(1200000000);
  return 0;
}