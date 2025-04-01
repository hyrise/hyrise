#include <iostream>
#include <thread>
#include <random>

#include "types.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  constexpr auto ELEMENT_COUNT = size_t{120'000'008};  // % 12 == 0
  auto values = std::vector<int32_t>(ELEMENT_COUNT);
  std::iota(values.begin(), values.end(), 0);

  auto random_device = std::random_device{};
  std::mt19937 g(random_device());

  std::ranges::shuffle(values, g);

  constexpr auto THREAD_COUNT = 12;
  constexpr auto PARTITION_SIZE = ELEMENT_COUNT / THREAD_COUNT;

  auto threads = std::vector<std::thread>{};
  for (auto thread_id = size_t{0}; thread_id < THREAD_COUNT; ++thread_id) {
    threads.emplace_back(std::thread([&, thread_id] {
      std::sort(values.begin() + thread_id * PARTITION_SIZE, values.begin() + ((thread_id + 1) * PARTITION_SIZE));
    }));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  std::cout << std::boolalpha << "Sorted? " << std::ranges::is_sorted(values) << " (should not be the case)\n";
  
  for (auto thread_id = size_t{0}; thread_id < THREAD_COUNT; ++thread_id) {
    std::cout << std::boolalpha << "Partitally sorted? " << std::is_sorted(values.begin() + thread_id * PARTITION_SIZE, values.begin() + ((thread_id + 1) * PARTITION_SIZE)) << " (should be the case)\n";
  }
}
