#include <iostream>

#include <cppcoro/recursive_generator.hpp>

#include "types.hpp"

using namespace opossum;  // NOLINT


cppcoro::recursive_generator<size_t> generator(size_t count) {
  for (auto index = size_t{0}; index < count; ++index) {
    co_yield index;
  }
}

cppcoro::recursive_generator<size_t> gengenerator(size_t count) {
  co_yield generator(17);
}

int main() {
  for (const auto index : generator(17)) {
  	std::cout << index << std::endl;
  }
}
