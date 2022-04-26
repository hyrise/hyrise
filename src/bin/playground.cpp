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

  const auto gen  = std::make_unique<cppcoro::recursive_generator<size_t>>(generator(17));
  cppcoro::recursive_generator<size_t>::iterator test = gen->begin();
  std::cout << *test << std::endl;
  ++test;
  ++test;
  ++test;
  ++test;
  std::cout << *test << std::endl;
}
