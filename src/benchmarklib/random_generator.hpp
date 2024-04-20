#pragma once

#include <random>
#include <set>
#include <string>

#include "utils/assert.hpp"

namespace hyrise {

class RandomGenerator {
 public:
  // Fix random seed by default, to make sure the benchmark is deterministic.
  explicit RandomGenerator(uint32_t seed = 42) : engine(seed) {}

  /**
   * Generates a random number between lower and upper.
   * @param lower       the lower bound
   * @param upper       the upper bound
   * @return            a random number
   */
  uint64_t random_number(uint64_t lower, uint64_t upper) {
    auto dist = std::uniform_int_distribution<uint64_t>{lower, upper};
    return dist(engine);
  }

  /**
   * Generates a set of unique size_ts in the half open interval [0, id_length). This function is used, e.g., to
   * generate foreign key relationships.
   * @param num_unique      number of unique values to be returned
   * @param id_length       maximum number (not included) in the set
   * @return                a set of unique numbers
   */
  std::set<size_t> select_unique_ids(size_t num_unique, size_t id_length) {
    auto rows = std::set<size_t>{};
    Assert(num_unique > 0, "Expected to select at least one ID.");
    Assert(num_unique <= id_length, "There are not enough IDs to be selected!");
    while (true) {
      rows.emplace(random_number(0, id_length - 1));

      if (rows.size() == num_unique) {
        return rows;
      }
    }
  }

 protected:
  std::default_random_engine engine;
};
}  // namespace hyrise
