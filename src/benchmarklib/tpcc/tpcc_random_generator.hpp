#pragma once

#include <algorithm>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <oneapi/tbb/concurrent_unordered_map.h>  // NOLINT(build/include_order): Identified as C system headers.

#include "random_generator.hpp"
#include "utils/assert.hpp"

namespace hyrise {

class TPCCRandomGenerator : public RandomGenerator {
 public:
  // Fix random seed by default, to make sure the benchmark is deterministic
  explicit TPCCRandomGenerator(uint32_t seed = 42) : RandomGenerator(seed) {}

  /**
   * Generates a random zip code as defined by TPCC
   * @return    the zip code as String
   */
  std::string zip_code() {
    return nstring(4, 4) + "11111";
  }

  /**
   * Generates a non-uniform random number based on a formula defined by TPCC.
   */
  size_t nurand(size_t a, size_t x, size_t y) {
    auto c_iter = _nurand_constants_c.find(a);
    if (c_iter == _nurand_constants_c.end()) {
      c_iter = _nurand_constants_c.insert({a, random_number(0, a)}).first;
    }
    const auto c = c_iter->second;
    return (((random_number(0, a) | random_number(x, y)) + c) % (y - x + 1)) + x;
  }

  /**
   * Generates a random last name based on a set of syllables.
   * @param i   given input i, a string is created in which each digit of i represents a syllable; if input i is larger
   *            than 999, use first create non-uniform random number between 255 and 1000
   * @return    a string representing the last name
   */
  std::string last_name(size_t i) {
    const std::string syllables[] = {
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
    };

    if (i >= 1000) {
      i = nurand(255, 0, 999);
    }

    std::string last_name("");
    last_name += syllables[(i / 100) % 10];
    last_name += syllables[(i / 10) % 10];
    last_name += syllables[i % 10];

    return last_name;
  }

  std::string generate_string(size_t lower_length, size_t upper_length, char base_character, int num_characters) {
    const auto length = random_number(lower_length, upper_length);
    auto result = std::string{};
    for (size_t i = 0; i < length; i++) {
      result.append(1, static_cast<char>(base_character + random_number(0, num_characters - 1)));
    }
    return result;
  }

  // Function and parameters as defined by TPCC. Generates alphanumeric string of random length.
  std::string astring(size_t lower_length, size_t upper_length) {
    return generate_string(lower_length, upper_length, 'a', 26);
  }

  // Function and parameters as defined by TPCC. Generates numeric string of random length.
  std::string nstring(size_t lower_length, size_t upper_length) {
    return generate_string(lower_length, upper_length, '0', 10);
  }

  std::vector<size_t> permutation(size_t lower, size_t upper) {
    auto values = std::vector<size_t>(upper - lower);
    std::iota(values.begin(), values.end(), lower);
    std::shuffle(values.begin(), values.end(), engine);
    return values;
  }

  // Reset nurand's C according to 2.1.6.1
  void reset_c_for_c_last() {
    const auto old_c = _nurand_constants_c.at(255);
    while (true) {
      _nurand_constants_c[255] = random_number(0, 255);
      const auto current_c = _nurand_constants_c.at(255);
      const auto diff = std::abs(static_cast<long>(current_c - old_c));  // NOLINT

      if (current_c != old_c && diff >= 64 && diff <= 120 && diff != 96 && diff != 112) {
        break;
      }
    }
  }

 protected:
  // Holds the constant C (see 2.1.6) for a given A. Is accessed concurrently.
  tbb::concurrent_unordered_map<size_t, size_t> _nurand_constants_c;
};
}  // namespace hyrise
