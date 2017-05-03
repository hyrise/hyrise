#pragma once

#include <random>
#include <set>
#include <string>

namespace opossum {

class RandomGenerator {
 public:
  static size_t number(size_t lower, size_t upper) {
    std::default_random_engine engine(std::random_device {}());
    std::uniform_int_distribution<size_t> dist(lower, upper);
    return dist(engine);
  }

  static std::string generate_last_name() {
    //            static const std::string syllables[] = {
    //                    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",};
    //            static const int LENGTHS[] = {3, 5, 4, 3, 4, 3, 4, 5, 5, 4,};

    return "";
  }

  static std::string generateString(size_t lower_length, size_t upper_length, char base_character, int num_characters) {
    //            static const char alphanum[] =
    //                "abcdefghijklmnopqrstuvwxyz"
    //                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    //                "0123456789";
    //            int stringLength = sizeof(alphanum) - 1;

    size_t length = number(lower_length, upper_length);
    std::string s;
    for (size_t i = 0; i < length; i++) {
      s.append(1, static_cast<char>(base_character + number(0, num_characters - 1)));
    }

    return s;
  }

  static std::string astring(size_t lower_length, size_t upper_length) {
    return RandomGenerator::generateString(lower_length, upper_length, 'a', 26);
  }

  static std::string nstring(size_t lower_length, size_t upper_length) {
    return RandomGenerator::generateString(lower_length, upper_length, '0', 10);
  }

  static std::set<size_t> select_unique_ids(size_t num_unique, size_t lower_id, size_t upper_id) {
    std::set<size_t> rows;

    //    std::cout << num_unique << " - " << lower_id << " - " << upper_id << std::endl;
    for (size_t i = 0; i < num_unique; ++i) {
      size_t index = static_cast<size_t>(-1);
      do {
        index = RandomGenerator::number(lower_id, upper_id);

        //        std::cout << index << std::endl;
      } while (rows.find(index) != rows.end());
      //      std::cout << "inserting" << std::endl;
      rows.insert(index);
    }
    assert(rows.size() == num_unique);
    return rows;
  }
};
}  // namespace opossum
