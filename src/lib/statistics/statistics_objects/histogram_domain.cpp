#include "histogram_domain.hpp"

#include "utils/assert.hpp"

namespace opossum {

HistogramDomain<pmr_string>::HistogramDomain()
    :  // Support most of ASCII with maximum prefix length for number of characters. The character range and the prefix
      // length were chosen so that the entire range of IntegralType is covered
      HistogramDomain<pmr_string>(' ', '~', 9) {}

HistogramDomain<pmr_string>::HistogramDomain(const char init_min_char, const char init_max_char,
                                             const size_t init_prefix_length)
    : min_char(init_min_char), max_char(init_max_char), prefix_length(init_prefix_length) {
  Assert(min_char <= max_char, "Invalid character range");
  Assert(prefix_length > 0, "String prefix too short");
}

size_t HistogramDomain<pmr_string>::character_range_width() const {
  // One cast at a time to suppress gcc warnings: https://stackoverflow.com/a/27513865
  return static_cast<size_t>(static_cast<unsigned char>(max_char - min_char + 1));
}

HistogramDomain<pmr_string>::IntegralType HistogramDomain<pmr_string>::string_to_number(
    const pmr_string& string_value) const {
  // The prefix length must not overflow for the number of supported characters when representing strings as numbers.
  DebugAssert(prefix_length < std::log(std::numeric_limits<uint64_t>::max()) / std::log(character_range_width() + 1),
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

pmr_string HistogramDomain<pmr_string>::string_to_domain(const pmr_string& string_value) const {
  auto converted = string_value;
  for (auto pos = size_t{0}; pos < converted.size(); ++pos) {
    converted[pos] = std::min(max_char, std::max(min_char, converted[pos]));
  }
  return converted;
}

bool HistogramDomain<pmr_string>::contains(const pmr_string& string_value) const {
  for (const auto char_value : string_value) {
    if (char_value > max_char || char_value < min_char) {
      return false;
    }
  }
  return true;
}

pmr_string HistogramDomain<pmr_string>::next_value_clamped(const pmr_string& string_in_domain) const {
  DebugAssert(contains(string_in_domain), "Unsupported character, cannot compute next_value()");

  // If the value is shorter than the prefix length, simply append the first supported character and return.
  if (string_in_domain.length() < prefix_length) {
    return string_in_domain + min_char;
  }

  // Special case: return `value` if it is the last supported one.
  if (string_in_domain == pmr_string(prefix_length, max_char)) {
    return string_in_domain;
  }

  // Otherwise, work on the substring trimmed to `prefix_length` characters.
  const auto value_clipped_to_prefix_length = string_in_domain.substr(0, prefix_length);
  const auto last_char = value_clipped_to_prefix_length.back();
  const auto substring = value_clipped_to_prefix_length.substr(0, value_clipped_to_prefix_length.length() - 1);

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
  return StringHistogramDomain{min_char, max_char, prefix_length - 1}.next_value_clamped(substring);
}

bool HistogramDomain<pmr_string>::operator==(const StringHistogramDomain& rhs) const {
  return min_char == rhs.min_char && max_char == rhs.max_char && prefix_length == rhs.prefix_length;
}

HistogramDomain<pmr_string>::IntegralType HistogramDomain<pmr_string>::_base_number() const {
  auto result = uint64_t{1};
  for (auto exp = uint64_t{1}; exp < prefix_length; exp++) {
    result += ipow(character_range_width(), exp);
  }

  return result;
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

}  // namespace opossum
