#include <array>
#include <chrono>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <list>
#include <memory_resource>

#include "types.hpp"

using namespace opossum;  // NOLINT

template <typename Func>
auto benchmark(Func test_func, int iterations) {
  const auto start = std::chrono::system_clock::now();
  while (iterations-- > 0) {
    test_func();
  }
  const auto stop = std::chrono::system_clock::now();
  const auto secs = std::chrono::duration<double>(stop - start);
  return secs.count();
}

int main() {
  constexpr int iterations{100};
  constexpr int total_nodes{2'00'000};

  auto default_std_alloc = [] {
    std::list<int> list;
    for (int i{}; i != total_nodes; ++i) {
      list.push_back(i);
    }
  };

  auto default_pmr_alloc = [] {
    std::pmr::list<int> list;
    for (int i{}; i != total_nodes; ++i) {
      list.push_back(i);
    }
  };

  auto pmr_alloc_no_buf = [] {
    std::pmr::monotonic_buffer_resource mbr;
    std::pmr::polymorphic_allocator<int> pa{&mbr};
    std::pmr::list<int> list{pa};
    for (int i{}; i != total_nodes; ++i) {
      list.push_back(i);
    }
  };

  auto pmr_alloc_and_buf = [] {
    std::array<std::byte, total_nodes * 32> buffer;  // enough to fit in all nodes
    std::pmr::monotonic_buffer_resource mbr{buffer.data(), buffer.size()};
    std::pmr::polymorphic_allocator<int> pa{&mbr};
    std::pmr::list<int> list{pa};
    for (int i{}; i != total_nodes; ++i) {
      list.push_back(i);
    }
  };

  const double t1 = benchmark(default_std_alloc, iterations);
  const double t2 = benchmark(default_pmr_alloc, iterations);
  const double t3 = benchmark(pmr_alloc_no_buf, iterations);
  const double t4 = benchmark(pmr_alloc_and_buf, iterations);

  std::cout << std::fixed << std::setprecision(3) << "t1 (default std alloc): " << t1 << " sec; t1/t1: " << t1 / t1
            << '\n'
            << "t2 (default pmr alloc): " << t2 << " sec; t1/t2: " << t1 / t2 << '\n'
            << "t3 (pmr alloc  no buf): " << t3 << " sec; t1/t3: " << t1 / t3 << '\n'
            << "t4 (pmr alloc and buf): " << t4 << " sec; t1/t4: " << t1 / t4 << '\n';
}
