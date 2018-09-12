#pragma once

#include <cmath>

#include <optional>
#include <string>
#include <utility>

namespace opossum {

template <typename T>
std::enable_if_t<std::is_integral_v<T>, T> previous_value(const T value) {
  return value - 1;
}

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, T> previous_value(const T value) {
  return std::nextafter(value, -std::numeric_limits<T>::infinity());
}

template <typename T>
std::enable_if_t<std::is_integral_v<T>, T> next_value(const T value) {
  return value + 1;
}

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, T> next_value(const T value) {
  return std::nextafter(value, std::numeric_limits<T>::infinity());
}

/**
 * Return the next representable string after `value` in the domain of strings with
 * at most length `string_prefix_length` and the possible character set of `supported_characters`.
 */
std::string next_value(const std::string& value, const std::string& supported_characters,
                       const uint64_t string_prefix_length);

/**
 * Return the next representable string after `value` in the domain of all strings with the
 * possible character set of `supported_characters`.
 * That is, append the first supported character to `value`.
 */
std::string next_value(const std::string& value, const std::string& supported_characters);

/**
 * Returns the power of base to exp.
 * The standard library function works on doubles and is inaccurate for large numbers.
 */
uint64_t ipow(uint64_t base, uint64_t exp);

/**
 * Returns the number of possible strings with at most length `string_prefix_length` - 1
 * in the possible character set of `supported_characters`.
 */
uint64_t base_value_for_prefix_length(const uint64_t string_prefix_length, const std::string& supported_characters);

/**
 * Returns the numerical representation of a string in the domain of all strings
 * with at most length `string_prefix_length` and in the possible character set of `supported_characters`.
 * The numerical representation is the number of possible strings in that domain alphabetically smaller than `value`.
 */
uint64_t convert_string_to_number_representation(const std::string& value, const std::string& supported_characters,
                                                 const uint64_t string_prefix_length);

/**
 * Returns the string for a numerical representation of a string in the domain of all strings
 * with at most length `string_prefix_length` and in the possible character set of `supported_characters`.
 */
std::string convert_number_representation_to_string(const uint64_t value, const std::string& supported_characters,
                                                    const uint64_t string_prefix_length);

/**
 * Returns a pair of supported characters and prefix length.
 * If no prefix length is supplied, it will return the maximum possible prefix length for the supported characters.
 * If no set of supported characters is given, it will return ASCII characters 32 - 126,
 * which are the vast majority of printable ASCII characters, and the maximum prefix length for that set (9).
 */
std::pair<std::string, uint64_t> get_default_or_check_prefix_settings(
    const std::optional<std::string>& supported_characters = std::nullopt,
    const std::optional<uint64_t>& string_prefix_length = std::nullopt);

/**
 * Checks whether a set of characters is sorted and does not have any gaps.
 */
bool check_string_sorted_and_without_gaps(const std::string& str);

/**
 * Checks whether the set of supported characters is longer than 1 and sorted.
 */
bool check_prefix_settings(const std::string& supported_characters);

/**
 * Checks that the prefix length is valid for the set of supported characters.
 * Also checks the supported characters by calling check_prefix_settings() for the supported_characters alone.
 */
bool check_prefix_settings(const std::string& supported_characters, const uint64_t string_prefix_length);

/**
 * Returns the length of the common prefix of `string1` and `string2`.
 */
uint64_t common_prefix_length(const std::string& string1, const std::string& string2);

}  // namespace opossum
