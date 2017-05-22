#pragma once

#include <iomanip>
#include <sstream>
#include <string>
#include <vector>
#include "grammar.hpp"
#include "random_generator.hpp"

namespace tpch {

class TextFieldGenerator {
 public:
  // TODO(anybody) change text size to 300MB: 300e6
  explicit TextFieldGenerator(RandomGenerator random_generator)
      : _random_gen(random_generator), _text(Grammar(random_generator).text(300e3)) {}

  std::string text_string(size_t lower_length, size_t upper_length) {
    auto length = _random_gen.number(lower_length, upper_length);
    auto start = _random_gen.number(0, _text.size() - length);
    return _text.substr(start, length);
  }

  std::string v_string(size_t lower_length, size_t upper_length) {
    size_t length = _random_gen.number(lower_length, upper_length);
    std::string s;
    for (size_t i = 0; i < length; i++) {
      auto offset = _random_gen.number(0, 63);
      size_t char_index;
      if (offset < 10) {
        char_index = '0' + offset;
      } else {
        offset -= 10;
        if (offset < 26) {
          char_index = 'a' + offset;
        } else {
          offset -= 26;
          if (offset < 26) {
            char_index = 'A' + offset;
          } else {
            offset -= 26;
            if (offset == 0) {
              char_index = '.';
            } else {
              char_index = ' ';
            }
          }
        }
      }
      s.append(1, static_cast<char>(char_index));
    }

    return s;
  }

  std::string phone_number(size_t nationkey) {
    std::stringstream ss;
    ss << nationkey + 10 << "-" << _random_gen.number(100, 999) << "-" << _random_gen.number(100, 999);
    ss << "-" << _random_gen.number(1000, 9999);
    return ss.str();
  }

  std::string fixed_length(size_t number, size_t length) {
    std::stringstream ss;
    ss << std::setfill('0') << std::setw(length) << number;
    return ss.str();
  }

  std::string part_name() {
    auto work_ids = _random_gen.select_unique_ids(5, part_name_words.size());
    auto it = work_ids.begin();
    std::string s(part_name_words[*it]);
    for (; it != work_ids.end(); it++) {
      s += " ";
      s += part_name_words[*it];
    }
    return s;
  }

  std::string part_type() {
    std::string s;
    s += word(part_type_syllables_1) + " ";
    s += word(part_type_syllables_2) + " ";
    s += word(part_type_syllables_3);
    return s;
  }

  std::string part_container() { return word(part_container_syllables_1) + " " + word(part_container_syllables_2); }

 protected:
  std::string word(std::vector<std::string> word_vector) {
    auto i = _random_gen.number(0, word_vector.size() - 1);
    return word_vector[i];
  }

  RandomGenerator _random_gen;
  const std::string _text;

  const std::vector<std::string> part_name_words = {
      "almond",    "antique",    "aquamarine", "azure",     "beige",     "bisque",     "black",     "blanched",
      "blue",      "blush",      "brown",      "burlywood", "burnished", "chartreuse", "chiffon",   "chocolate",
      "coral",     "cornflower", "cornsilk",   "cream",     "cyan",      "dark",       "deep",      "dim",
      "dodger",    "drab",       "firebrick",  "floral",    "forest",    "frosted",    "gainsboro", "ghost",
      "goldenrod", "green",      "grey",       "honeydew",  "hot",       "indian",     "ivory",     "khaki",
      "lace",      "lavender",   "lawn",       "lemon",     "light",     "lime",       "linen",     "magenta",
      "maroon",    "medium",     "metallic",   "midnight",  "mint",      "misty",      "moccasin",  "navajo",
      "navy",      "olive",      "orange",     "orchid",    "pale",      "papaya",     "peach",     "peru",
      "pink",      "plum",       "powder",     "puff",      "purple",    "red",        "rose",      "rosy",
      "royal",     "saddle",     "salmon",     "sandy",     "seashell",  "sienna",     "sky",       "slate",
      "smoke",     "snow",       "spring",     "steel",     "tan",       "thistle",    "tomato",    "turquoise",
      "violet",    "wheat",      "white",      "yellow"};

  const std::vector<std::string> part_type_syllables_1 = {"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"};
  const std::vector<std::string> part_type_syllables_2 = {"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"};
  const std::vector<std::string> part_type_syllables_3 = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};

  const std::vector<std::string> part_container_syllables_1 = {"SM", "LG", "MED", "JUMBO", "WRAP"};
  const std::vector<std::string> part_container_syllables_2 = {"CASE", "BOX",  "BAG", "JAR",
                                                               "PKG",  "PACK", "CAN", "DRUM"};
};
}  // namespace tpch
