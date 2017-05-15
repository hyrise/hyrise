#pragma once

#include <algorithm>
#include <cassert>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <vector>

namespace tpch {

class RandomGenerator {
 public:
  RandomGenerator() : engine(std::random_device()()) {}

  int32_t number(int32_t lower, int32_t upper) {
    std::uniform_int_distribution<int32_t> dist(lower, upper);
    return dist(engine);
  }

 protected:
  std::default_random_engine engine;
};
}  // namespace tpch
