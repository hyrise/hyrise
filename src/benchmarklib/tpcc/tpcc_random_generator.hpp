#pragma once

#include <algorithm>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "random_generator.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TPCCRandomGenerator : public opossum::RandomGenerator {
 public:
  // Fix random seed by default, to make sure the benchmark is deterministic
  explicit TPCCRandomGenerator(uint32_t seed = 42) : opossum::RandomGenerator(seed) {}

  /**
   * Generates a random zip code as defined by TPCC
   * @return    the zip code as String
   */
  std::string zip_code() { return nstring(4, 4) + "11111"; }

  /**
   * Generates a non-uniform random number based on a formula defined by TPCC
   */
  size_t nurand(size_t a, size_t x, size_t y) {
    Assert(_nurand_constant_c <= a, "Invalid param: a=" + std::to_string(a));
    return (((random_number(0, a) | random_number(x, y)) + _nurand_constant_c) % (y - x + 1)) + x;
  }

  /**
   * Generates a random last name based on a set of syllables
   * @param i   a number, if less than 1000 it each digit represents a syllable
   *            for i's greater than 1000 we calculate a non-uniform random number below 1000
   * @return    a String representing the last name
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
    size_t length = random_number(lower_length, upper_length);
    std::string s;
    for (size_t i = 0; i < length; i++) {
      s.append(1, static_cast<char>(base_character + random_number(0, num_characters - 1)));
    }
    return s;
  }

  // Function and parameters as defined by TPCC
  // Generates alphanumeric string of random length
  std::string astring(size_t lower_length, size_t upper_length) {
    return generate_string(lower_length, upper_length, 'a', 26);
  }

  // Function and parameters as defined by TPCC
  // Generates numeric string of random length
  std::string nstring(size_t lower_length, size_t upper_length) {
    return generate_string(lower_length, upper_length, '0', 10);
  }

  std::vector<size_t> permutation(size_t lower, size_t upper) {
    std::vector<size_t> v(upper - lower);
    std::iota(v.begin(), v.end(), lower);
    std::shuffle(v.begin(), v.end(), engine);
    return v;
  }

 protected:
  const size_t _nurand_constant_c = random_number(0, 255);
};
}  // namespace opossum
