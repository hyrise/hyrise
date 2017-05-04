#pragma once

#include <cassert>
#include <random>
#include <set>
#include <string>

namespace opossum {

class RandomGenerator {
 public:
  RandomGenerator() : engine(std::random_device()()) {}

  size_t number(size_t lower, size_t upper) {
    std::uniform_int_distribution<size_t> dist(lower, upper);
    return dist(engine);
  }

  std::string zipCode() { return nstring(4, 4) + "11111"; }

  std::string last_name() {
    //            static const std::string syllables[] = {
    //                    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",};
    //            static const int LENGTHS[] = {3, 5, 4, 3, 4, 3, 4, 5, 5, 4,};

    return "";
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

  std::set<size_t> select_unique_ids(size_t num_unique, size_t lower_id, size_t upper_id) {
    std::set<size_t> rows;

    for (size_t i = 0; i < num_unique; ++i) {
      size_t index = static_cast<size_t>(-1);
      do {
        index = number(lower_id, upper_id);
      } while (rows.find(index) != rows.end());
      rows.insert(index);
    }
    assert(rows.size() == num_unique);
    return rows;
  }

 protected:
  std::default_random_engine engine;
};
}  // namespace opossum
