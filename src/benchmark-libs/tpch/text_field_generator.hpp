#pragma once

#include <iomanip>
#include <sstream>
#include <string>
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

 protected:
  RandomGenerator _random_gen;
  const std::string _text;
};
}  // namespace tpch
