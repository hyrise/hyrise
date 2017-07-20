#pragma once

#include <cassert>
#include <random>
#include <set>
#include <string>

namespace benchmark_utilities {

class RandomGenerator {
 public:
  RandomGenerator(unsigned int seed = 42) : engine(seed) {}

  int32_t number(int32_t lower, int32_t upper) {
    std::uniform_int_distribution<int32_t> dist(lower, upper);
    return dist(engine);
  }

  std::string generateString(size_t lower_length, size_t upper_length, char base_character, int num_characters) {
    size_t length = number(lower_length, upper_length);
    std::string s;
    for (size_t i = 0; i < length; i++) {
      s.append(1, static_cast<char>(base_character + number(0, num_characters - 1)));
    }
    return s;
  }

  std::set<size_t> select_unique_ids(size_t num_unique, size_t id_length) {
    std::set<size_t> rows;

    for (size_t i = 0; i < num_unique; ++i) {
      size_t index = static_cast<size_t>(-1);
      do {
        index = number(0, id_length - 1);
      } while (rows.find(index) != rows.end());
      rows.insert(index);
    }
    assert(rows.size() == num_unique);
    return rows;
  }

 protected:
  std::default_random_engine engine;
};
}  // namespace benchmark_utilities
