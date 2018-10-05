#include "histogram_utils.hpp"

#include <cmath>

#include <algorithm>
#include <limits>
#include <string>
#include <utility>

#include "utils/assert.hpp"

using namespace opossum::histogram;  // NOLINT

namespace opossum {

std::string next_value(const std::string& value, const std::string& supported_characters,
                       const size_t string_prefix_length) {
  DebugAssert(value.find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");
  DebugAssert(check_string_sorted_and_without_gaps(supported_characters),
              "Supported characters must be sorted and without gaps.");

  // If the value is shorter than the prefix length, simply append the first supported character and return.
  if (value.length() < string_prefix_length) {
    return value + supported_characters.front();
  }

  // Special case: return `value` if it is the last supported one.
  if (value == std::string(string_prefix_length, supported_characters.back())) {
    return value;
  }

  // Otherwise, work on the substring trimmed to `string_prefix_length` characters.
  const auto cleaned_value = value.substr(0, string_prefix_length);
  const auto last_char = cleaned_value.back();
  const auto substring = cleaned_value.substr(0, cleaned_value.length() - 1);

  // If the last character of the substring is not the last supported character,
  // simply exchange it with the character following it.
  if (last_char != supported_characters.back()) {
    return substring + static_cast<char>(last_char + 1);
  }

  // Otherwise, remove the last character and return the next value of the string without the last character.
  // Example:
  // - supported_characters: [a-z]
  // - string_prefix_length: 4
  // - value: abcz
  // - next_value: abd
  return next_value(substring, supported_characters, string_prefix_length - 1);
}

std::string next_value(const std::string& value, const std::string& supported_characters) {
  return next_value(value, supported_characters, value.length() + 1);
}

uint64_t ipow(uint64_t base, uint64_t exp) {
  // Taken from https://stackoverflow.com/a/101613/2362807.
  // Note: this function does not check for any possible overflows!
  uint64_t result = 1;

  for (;;) {
    if (exp & 1u) {
      result *= base;
    }

    exp >>= 1u;

    if (!exp) {
      break;
    }

    base *= base;
  }

  return result;
}

namespace histogram {

uint64_t base_value_for_prefix_length(const size_t string_prefix_length, const std::string& supported_characters) {
  DebugAssert(string_prefix_length > 0, "Prefix length must be greater than 0.");

  auto result = uint64_t{1};
  for (auto exp = uint64_t{1}; exp < string_prefix_length; exp++) {
    result += ipow(supported_characters.length(), exp);
  }
  return result;
}

uint64_t convert_string_to_number_representation(const std::string& value, const std::string& supported_characters,
                                                 const size_t string_prefix_length) {
  DebugAssert(value.find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");

  if (value.empty()) {
    return 0;
  }

  DebugAssert(check_prefix_settings(supported_characters, string_prefix_length), "Invalid prefix settings.");

  const auto base = base_value_for_prefix_length(string_prefix_length, supported_characters);
  const auto trimmed = value.substr(0, string_prefix_length);
  const auto char_value = (trimmed.front() - supported_characters.front()) * base + 1;

  // If `value` is longer than `string_prefix_length` add 1 to the result.
  // This is required for the way EqualWidthHistograms calculate bin edges.
  return char_value +
         convert_string_to_number_representation(trimmed.substr(1, trimmed.length() - 1), supported_characters,
                                                 string_prefix_length - 1) +
         (value.length() > string_prefix_length ? 1 : 0);
}

std::string convert_number_representation_to_string(const uint64_t value, const std::string& supported_characters,
                                                    const size_t string_prefix_length) {
  DebugAssert(convert_string_to_number_representation(std::string(string_prefix_length, supported_characters.back()),
                                                      supported_characters, string_prefix_length) >= value,
              "Value is not in valid range for supported_characters and string_prefix_length.");

  if (value == 0ul) {
    return "";
  }

  const auto base = base_value_for_prefix_length(string_prefix_length, supported_characters);
  const auto character = supported_characters.at((value - 1) / base);
  return character +
         convert_number_representation_to_string((value - 1) % base, supported_characters, string_prefix_length - 1);
}

bool check_string_sorted_and_without_gaps(const std::string& str) {
  for (auto it = str.cbegin(); it < str.cend(); it++) {
    if (std::distance(str.cbegin(), it) != *it - str.front()) {
      return false;
    }
  }

  return true;
}

bool check_prefix_settings(const std::string& supported_characters) {
  if (supported_characters.length() <= 1) {
    return false;
  }

  // The supported characters string has to be sorted and without gaps
  // for the transformation from string to number to work according to the requirements.
  return check_string_sorted_and_without_gaps(supported_characters);
}

bool check_prefix_settings(const std::string& supported_characters, const size_t string_prefix_length) {
  if (!check_prefix_settings(supported_characters) || string_prefix_length == 0) {
    return false;
  }

  // The prefix length must not overflow for the number of supported characters when representing strings as numbers.
  return string_prefix_length <
         std::log(std::numeric_limits<uint64_t>::max()) / std::log(supported_characters.length() + 1);
}

std::pair<std::string, size_t> get_default_or_check_string_histogram_prefix_settings(
    const std::optional<std::string>& supported_characters, const std::optional<size_t>& string_prefix_length) {
  std::string characters;
  size_t prefix_length;

  if (supported_characters) {
    characters = *supported_characters;

    if (string_prefix_length) {
      prefix_length = *string_prefix_length;
    } else {
      prefix_length = static_cast<size_t>(63 / std::log(characters.length() + 1));
    }
  } else {
    DebugAssert(!string_prefix_length, "Cannot set prefix length without also setting supported characters.");

    // Support most of ASCII with maximum prefix length for number of characters.
    characters = " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
    prefix_length = 9;
  }

  DebugAssert(check_prefix_settings(characters, prefix_length), "Invalid prefix settings.");

  return {characters, prefix_length};
}

uint64_t common_prefix_length(const std::string& string1, const std::string& string2) {
  const auto string_length = std::min(string1.length(), string2.length());
  auto common_prefix_length = 0u;
  for (; common_prefix_length < string_length; common_prefix_length++) {
    if (string1[common_prefix_length] != string2[common_prefix_length]) {
      break;
    }
  }

  return common_prefix_length;
}

}  // namespace histogram

}  // namespace opossum
