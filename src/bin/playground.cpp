#include <iostream>

#include "types.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  const auto world = pmr_string{"world"};
  std::cout << "Hello " << world << "!\n";
  return 0;
}
