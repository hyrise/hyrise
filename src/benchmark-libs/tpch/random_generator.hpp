#pragma once

#include <random>
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

  std::string generateString(size_t lower_length, size_t upper_length, char base_character, int num_characters) {
    size_t length = number(lower_length, upper_length);
    std::string s;
    for (size_t i = 0; i < length; i++) {
      s.append(1, static_cast<char>(base_character + number(0, num_characters - 1)));
    }

    return s;
  }

 protected:
  std::default_random_engine engine;
};
}  // namespace tpch
