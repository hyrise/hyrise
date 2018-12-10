#include <iostream>
#include <bitset>

#include "statistics/chunk_statistics/counting_quotient_filter.hpp"

#include "types.hpp"

using namespace opossum;  // NOLINT

template <typename T>
void hashish(T value) {
	size_t hash = std::hash<T>{}(value);
	size_t fibo = (std::hash<T>{}(value) * 11400714819323198485llu);
	size_t fibo2 = CountingQuotientFilter<T>::get_hash_bits(static_cast<T>(value), 64);
	std::cout << value << " >>\n\t\t" << std::bitset<64>(hash) << "\n\t\t" << std::bitset<64>(fibo) << "\n\t\t" << std::bitset<64>(fibo2) << std::endl << std::endl;
}

int main() {
  std::cout << "Floats" << std::endl;
  hashish(1.2f);
  hashish(12.0f);
  hashish(120.0f);
  hashish(123.0f);
  hashish(1200000000.0f);

  std::cout << "Doubles" << std::endl;
  hashish(1.2);
  hashish(12.0);
  hashish(120.0);
  hashish(123.0);
  hashish(1200000000.0);

  std::cout << "Integers" << std::endl;
  hashish(1);
  hashish(12);
  hashish(120);
  hashish(123);
  hashish(1200000000);

  return 0;
}