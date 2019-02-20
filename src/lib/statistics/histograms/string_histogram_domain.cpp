#include "string_histogram_domain.hpp"

#include "histogram_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

StringHistogramDomain::StringHistogramDomain()
    :  // Support most of ASCII with maximum prefix length for number of characters.
      StringHistogramDomain(
          " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~", 9) {}

StringHistogramDomain::StringHistogramDomain(const std::string& supported_characters, const size_t prefix_length)
    : supported_characters(supported_characters), prefix_length(prefix_length) {
  DebugAssert(!supported_characters.empty(), "Need at least one supported character");
  DebugAssert(prefix_length > 0, "String prefix too short");

  for (auto idx = size_t{0}; idx < supported_characters.size(); ++idx) {
    DebugAssert(idx == static_cast<size_t>(supported_characters[idx] - supported_characters.front()),
                "The supported characters string has to be sorted and without gaps");
  }
}

std::string StringHistogramDomain::number_to_string(IntegralType int_value) const {
  // The prefix length must not overflow for the number of supported characters when representing strings as numbers.
  DebugAssert(
      prefix_length < std::log(std::numeric_limits<uint64_t>::max()) / std::log(supported_characters.length() + 1),
      "String prefix too long");
  DebugAssert(string_to_number(std::string(prefix_length, supported_characters.back())) >= int_value,
              "Value is not in valid range for supported_characters and prefix_length.");

  std::string string_value;

  auto base = base_number();

  auto idx = 0;

  while (int_value > 0) {
    string_value += supported_characters.at((int_value - 1) / base);
    int_value = (int_value - 1) % base;
    base -= ipow(supported_characters.size(), prefix_length - idx - 1);
    ++idx;
  }

  return string_value;
}

StringHistogramDomain::IntegralType StringHistogramDomain::string_to_number(const std::string& string_value) const {
  // The prefix length must not overflow for the number of supported characters when representing strings as numbers.
  DebugAssert(
      prefix_length < std::log(std::numeric_limits<uint64_t>::max()) / std::log(supported_characters.length() + 1),
      "String prefix too long");
  if (string_value.find_first_not_of(supported_characters) != std::string::npos) {
    return string_to_number(string_to_domain(string_value));
  }

  auto base = base_number();
  auto value = IntegralType{0};

  for (auto idx = size_t{0}; idx < std::min(string_value.size(), prefix_length); ++idx) {
    value += (string_value[idx] - supported_characters.front()) * base + 1;
    base -= ipow(supported_characters.size(), prefix_length - idx - 1);
  }

  // If `value` is longer than `prefix_length` add 1 to the result.
  // This is required for the way EqualWidthHistograms calculate bin edges.
  value += string_value.length() > prefix_length ? 1 : 0;

  return value;
}

std::string StringHistogramDomain::string_to_domain(const std::string& string_value) const {
  auto converted = string_value;
  auto pos = size_t{0};

  const auto min = supported_characters.front();
  const auto max = supported_characters.back();

  while ((pos = converted.find_first_not_of(supported_characters, pos)) != std::string::npos) {
    converted[pos] = std::min(max, std::max(min, converted[pos]));
  }

  return converted;
}

bool StringHistogramDomain::contains(const std::string &string_value) const {
  return string_value.find_first_not_of(supported_characters) == std::string::npos;
}

bool StringHistogramDomain::is_valid_prefix(const std::string& string_value) const {
  return contains(string_value) && string_value.size() <= prefix_length;
}

std::string StringHistogramDomain::next_value(const std::string &string_value) const {
  DebugAssert(contains(string_value), "Unsupported character, cannot compute next_value()");

  // If the value is shorter than the prefix length, simply append the first supported character and return.
  if (string_value.length() < prefix_length) {
    return string_value + supported_characters.front();
  }

  // Special case: return `value` if it is the last supported one.
  if (string_value == std::string(prefix_length, supported_characters.back())) {
    return string_value;
  }

  // Otherwise, work on the substring trimmed to `prefix_length` characters.
  const auto cleaned_value = string_value.substr(0, prefix_length);
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
  // - prefix_length: 4
  // - value: abcz
  // - next_value: abd
  return StringHistogramDomain{supported_characters, prefix_length - 1}.next_value(substring);
}
std::string StringHistogramDomain::previous_value(const std::string& string_value) const {
  Assert(is_valid_prefix(string_value), "Can only compute previous_value() of valid prefixes");

  const auto number = string_to_number(string_value);

  if (number == 0) {
    return string_value;
  } else {
    return number_to_string(number - 1u);
  }
}

StringHistogramDomain::IntegralType StringHistogramDomain::base_number() const {
  DebugAssert(prefix_length > 0, "Prefix length must be greater than 0.");

  auto result = uint64_t{1};
  for (auto exp = uint64_t{1}; exp < prefix_length; exp++) {
    result += ipow(supported_characters.length(), exp);
  }

  return result;
}

bool StringHistogramDomain::operator==(const StringHistogramDomain& rhs) const {
  return supported_characters == rhs.supported_characters && prefix_length == rhs.prefix_length;
}

}  // namespace opossum
