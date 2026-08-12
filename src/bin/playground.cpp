#include <iostream>

#include "types.hpp"

int main() {
  using namespace hyrise;  // NOLINT(build/namespaces)

  const auto world = pmr_string{"world"};
  std::cout << "Hello " << world << "!\n";
  return 0;
}
