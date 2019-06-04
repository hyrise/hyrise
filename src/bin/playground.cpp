#include <iostream>

#include <random>

#include "sparsehash/dense_hash_map"

#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_hashsort.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "storage/storage_manager.hpp"
#include "table_generator.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "utils/timer.hpp"

using namespace opossum;  // NOLINT

struct Hash {
  int * x = nullptr;

  template<typename T>
  size_t operator()(const T& p) const {
    return p.first;
  }
};

int main() {
  auto hash = Hash{};
  auto m = google::dense_hash_map<std::pair<int, int>, std::string, Hash>{5, hash};
  m.set_empty_key(std::pair<int, int>{9, 9});

  m.insert(std::pair{std::pair<int, int>{3, 4}, "hello"});
  m.insert(std::pair{std::pair<int, int>{1, 4}, "world"});

  std::cout << m[std::pair<int, int>{3, 4}] << " " << m[std::pair<int, int>{1, 4}] << std::endl;

  return 0;
}
