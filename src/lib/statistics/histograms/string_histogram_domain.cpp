#include "string_histogram_domain.hpp"

#include "histogram_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

StringHistogramDomain::StringHistogramDomain()
    :  // Support most of ASCII with maximum prefix length for number of characters.
      StringHistogramDomain(' ', '~', 9) {}

StringHistogramDomain::StringHistogramDomain(const char min_char, const char max_char, const size_t prefix_length)
    : min_char(min_char), max_char(max_char), prefix_length(prefix_length) {
  Assert(min_char <= max_char, "Invalid character range");
  Assert(prefix_length > 0, "String prefix too short");
}

size_t StringHistogramDomain::character_range_width() const {
  return static_cast<size_t>(max_char - min_char + 1);
}

std::string StringHistogramDomain::number_to_string(IntegralType int_value) const {
  // The prefix length must not overflow for the number of supported characters when representing strings as numbers.
  DebugAssert(
      prefix_length < std::log(std::numeric_limits<uint64_t>::max()) / std::log(character_range_width() + 1),
      "String prefix too long");
  DebugAssert(string_to_number(std::string(prefix_length, max_char)) >= int_value,
              "Value is not in valid range for supported_characters and prefix_length.");

  std::string string_value;

  auto base = _base_number();

  auto idx = 0;

  while (int_value > 0) {
    string_value += static_cast<char>(min_char + (int_value - 1) / base);
    int_value = (int_value - 1) % base;
    base -= ipow(character_range_width(), prefix_length - idx - 1);
    ++idx;
  }

  return string_value;
}

StringHistogramDomain::IntegralType StringHistogramDomain::string_to_number(const std::string& string_value) const {
  // The prefix length must not overflow for the number of supported characters when representing strings as numbers.
  DebugAssert(
      prefix_length < std::log(std::numeric_limits<uint64_t>::max()) / std::log(character_range_width() + 1),
      "String prefix too long");
  if (!contains(string_value)) {
    return string_to_number(string_to_domain(string_value));
  }

  auto base = _base_number();
  auto value = IntegralType{0};

  for (auto idx = size_t{0}; idx < std::min(string_value.size(), prefix_length); ++idx) {
    value += (string_value[idx] - min_char) * base + 1;
    base -= ipow(character_range_width(), prefix_length - idx - 1);
  }

  // If `value` is longer than `prefix_length` add 1 to the result.
  // This is required for the way EqualWidthHistograms calculate bin edges.
  value += string_value.length() > prefix_length ? 1 : 0;

  return value;
}

std::string StringHistogramDomain::string_to_domain(const std::string& string_value) const {
  auto converted = string_value;
  for (auto pos = size_t{0}; pos < converted.size(); ++pos) {
    converted[pos] = std::min(max_char, std::max(min_char, converted[pos]));
  }
  return converted;
}

bool StringHistogramDomain::contains(const std::string &string_value) const {
  for (auto pos = size_t{0}; pos < string_value.size(); ++pos) {
    if (string_value[pos] > max_char || string_value[pos] < min_char) {
      return false;
    }
  }
  return true;
}

bool StringHistogramDomain::is_valid_prefix(const std::string& string_value) const {
  return contains(string_value) && string_value.size() <= prefix_length;
}

std::string StringHistogramDomain::next_value(const std::string &string_value) const {
  DebugAssert(contains(string_value), "Unsupported character, cannot compute next_value()");

  // If the value is shorter than the prefix length, simply append the first supported character and return.
  if (string_value.length() < prefix_length) {
    return string_value + min_char;
  }

  // Special case: return `value` if it is the last supported one.
  if (string_value == std::string(prefix_length, max_char)) {
    return string_value;
  }

  // Otherwise, work on the substring trimmed to `prefix_length` characters.
  const auto cleaned_value = string_value.substr(0, prefix_length);
  const auto last_char = cleaned_value.back();
  const auto substring = cleaned_value.substr(0, cleaned_value.length() - 1);

  // If the last character of the substring is not the last supported character,
  // simply exchange it with the character following it.
  if (last_char != max_char) {
    return substring + static_cast<char>(last_char + 1);
  }

  // Otherwise, remove the last character and return the next value of the string without the last character.
  // Example:
  // - supported_characters: [a-z]
  // - prefix_length: 4
  // - value: abcz
  // - next_value: abd
  return StringHistogramDomain{min_char, max_char, prefix_length - 1}.next_value(substring);
}

bool StringHistogramDomain::operator==(const StringHistogramDomain& rhs) const {
  return min_char == rhs.min_char && max_char == rhs.max_char && prefix_length == rhs.prefix_length;
}

StringHistogramDomain::IntegralType StringHistogramDomain::_base_number() const {
  DebugAssert(prefix_length > 0, "Prefix length must be greater than 0.");

  auto result = uint64_t{1};
  for (auto exp = uint64_t{1}; exp < prefix_length; exp++) {
    result += ipow(character_range_width(), exp);
  }

  return result;
}

}  // namespace opossum
