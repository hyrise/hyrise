#pragma once

#include <random>
#include <set>
#include <string>

#include "utils/assert.hpp"

namespace benchmark_utilities {

class RandomGenerator {
 public:
  // Fix random seed by default, to make sure the benchmark is deterministic
  explicit RandomGenerator(uint32_t seed = 42) : engine(seed) {}

  /**
   * Generates a random number between lower and upper.
   * @param lower       the lower bound
   * @param upper       the upper bound
   * @return            a random number
   */
  template <class IntType = uint32_t, typename LowerType, typename UpperType>
  IntType random_number(LowerType lower, UpperType upper) {
    std::uniform_int_distribution<IntType> dist(lower, upper);
    return dist(engine);
  }

  /**
   * Generates a set of unique ints with a defined length.
   * This function is used, e.g., to generate foreign key relationships
   * @param num_unique      number of unique values to be returned
   * @param id_length       maximum number in the set
   * @return                a set of unique numbers
   */
  std::set<size_t> select_unique_ids(size_t num_unique, size_t id_length) {
    std::set<size_t> rows;
    Assert(num_unique <= id_length, "There are not enough ids to be selected!");
    for (size_t i = 0; i < num_unique; ++i) {
      size_t index = static_cast<size_t>(-1);
      do {
        index = random_number(0, id_length - 1);
      } while (rows.find(index) != rows.end());
      rows.insert(index);
    }
    return rows;
  }

 protected:
  std::default_random_engine engine;
};
}  // namespace benchmark_utilities
