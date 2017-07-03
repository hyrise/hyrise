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

  size_t number(size_t lower, size_t upper) {
    std::uniform_int_distribution<size_t> dist(lower, upper);
    return dist(engine);
  }

  std::string zipCode() { return nstring(4, 4) + "11111"; }

  size_t nurand(size_t a, size_t x, size_t y) {
    assert(0 <= _c && _c <= a);
    return (((number(0, a) | number(x, y)) + _c) % (y - x + 1)) + x;
  }

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
