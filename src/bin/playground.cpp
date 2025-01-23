#include <iostream>
#include <memory_resource>

#include "types.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  auto resource = std::pmr::new_delete_resource();
  auto v = std::pmr::vector<int>{resource};

  const auto n = 10;
  for (int i = 0; i < n; ++i) {
    v.push_back(i);
  }

  for (int i = 0; i < n; ++i) {
    printf("%i\n", v[i]);
  }

  return 0;
}
