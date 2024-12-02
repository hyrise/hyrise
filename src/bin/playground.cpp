#include <iostream>

#include <boost/unordered/unordered_flat_map.hpp>

#include "types.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  for (auto measurement_id = size_t{0}; measurement_id < 20; ++measurement_id) {
    auto unreserved = boost::unordered_flat_map<pmr_string, ChunkOffset>{};
    auto reserved = boost::unordered_flat_map<pmr_string, ChunkOffset>{};
    reserved.reserve(100'000);

    const auto start_1 = std::chrono::system_clock::now();
    for (auto i = size_t{0}; i < 100'000; ++i) {
      unreserved.emplace(std::to_string(i), ChunkOffset{i});
    }
    const auto end_1 = std::chrono::system_clock::now();
    std::cerr << "UNRES\t" << std::chrono::duration<double, std::micro>(end_1 - start_1).count() << " us.\n";

    const auto start_2 = std::chrono::system_clock::now();
    for (auto i = size_t{0}; i < 100'000; ++i) {
      reserved.emplace(std::to_string(i), ChunkOffset{i});
    }
    const auto end_2 = std::chrono::system_clock::now();
    std::cerr << "RES  \t" << std::chrono::duration<double, std::micro>(end_2 - start_2).count() << " us.\n";
  }
  return 0;
}
