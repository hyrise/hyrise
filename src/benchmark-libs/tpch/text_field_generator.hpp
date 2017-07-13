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
  explicit TextFieldGenerator(RandomGenerator random_generator)
      : _random_gen(random_generator), _text(Grammar(random_generator).text(300e2)) {} //TODO(Jonathan) set back to 300e6 (300MB)

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

  std::string customer_segment() { return word(customer_segments); }

  std::string order_priority() { return word(order_priorities); }

  std::string lineitem_instruction() { return word(lineitem_instructions); }

  std::string lineitem_mode() { return word(lineitem_modes); }

  const std::vector<std::string> nation_names = {
      "ALGERIA", "ARGENTINA", "BRAZIL",         "CANADA",       "EGYPT", "ETHIOPIA", "FRANCE",
      "GERMANY", "INDIA",     "INDONESIA",      "IRAN",         "IRAQ",  "JAPAN",    "JORDAN",
      "KENYA",   "MOROCCO",   "MOZAMBIQUE",     "PERU",         "CHINA", "ROMANIA",  "SAUDI ARABIA",
      "VIETNAM", "RUSSIA",    "UNITED KINGDOM", "UNITED STATES"};

  const std::vector<std::string> region_names = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};

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

  const std::vector<std::string> customer_segments = {"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"};

  const std::vector<std::string> order_priorities = {"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"};

  const std::vector<std::string> lineitem_instructions = {"DELIVER IN PERSON", "COLLECT COD", "NONE",
                                                          "TAKE BACK RETURN"};

  const std::vector<std::string> lineitem_modes = {"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"};
};
}  // namespace tpch
