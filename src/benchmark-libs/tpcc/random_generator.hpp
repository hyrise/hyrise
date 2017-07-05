#pragma once

#include <algorithm>
#include <cassert>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <vector>

namespace tpcc {

class RandomGenerator {
 public:
  // Fix random seed, to make sure the benchmark is deterministic
  RandomGenerator() : engine(42) {}

  /**
   * Generates a random number between lower and upper.
   * @param lower       the lower bound
   * @param upper       the upper bound
   * @return            a random number
   */
  size_t number(size_t lower, size_t upper) {
    std::uniform_int_distribution<size_t> dist(lower, upper);
    return dist(engine);
  }

  /**
   * Generates a random zip code as defined by TPCC
   * @return    the zip code as String
   */
  std::string zipCode() { return nstring(4, 4) + "11111"; }

  /**
   * Generates a non-uniform random number based on a formula defined by TPCC
   */
  size_t nurand(size_t a, size_t x, size_t y) {
    assert(0 <= _c && _c <= a);
    return (((number(0, a) | number(x, y)) + _c) % (y - x + 1)) + x;
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

  std::string generateString(size_t lower_length, size_t upper_length, char base_character, int num_characters) {
    size_t length = number(lower_length, upper_length);
    std::string s;
    for (size_t i = 0; i < length; i++) {
      s.append(1, static_cast<char>(base_character + number(0, num_characters - 1)));
    }

    return s;
  }

  std::string astring(size_t lower_length, size_t upper_length) {
    return generateString(lower_length, upper_length, 'a', 26);
  }

  std::string nstring(size_t lower_length, size_t upper_length) {
    return generateString(lower_length, upper_length, '0', 10);
  }

  /**
   * Generates a set of unique ints with a defined length.
   * This function is used, e.g., to generate foreign key relationships
   * @param num_unique      number of unique values to be returned
   * @param max_id          maximum number in the set
   * @return                a set of unique numbers
   */
  std::set<size_t> select_unique_ids(size_t num_unique, size_t max_id) {
    std::set<size_t> rows;

    for (size_t i = 0; i < num_unique; ++i) {
      size_t index;
      do {
        index = number(0, max_id - 1);
      } while (rows.find(index) != rows.end());
      rows.insert(index);
    }
    assert(rows.size() == num_unique);
    return rows;
  }

  std::vector<size_t> permutation(size_t lower, size_t upper) {
    std::vector<size_t> v(upper - lower);
    std::iota(v.begin(), v.end(), lower);
    std::shuffle(v.begin(), v.end(), engine);
    return v;
  }

 protected:
  std::default_random_engine engine;
  const size_t _c = number(0, 255);
};
}  // namespace tpcc
