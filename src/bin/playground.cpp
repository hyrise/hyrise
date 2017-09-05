#include <iostream>

#include "polymorphic_allocator.hpp"
#include "types.hpp"
#include "utils/numa_memory_resource.hpp"

int main() {
  auto memory_resource = opossum::NUMAMemoryResource(2, "test");
  const auto alloc = opossum::PolymorphicAllocator<size_t>(&memory_resource);
  {
    auto vec = opossum::alloc_vector<size_t>(1024, alloc);
    std::cout << "Welp" << std::endl;
    for (size_t i = 0; i < 1024; i++) {
      vec[i] = i;
    }
  }
  std::cout << "WAT" << std::endl;
}
