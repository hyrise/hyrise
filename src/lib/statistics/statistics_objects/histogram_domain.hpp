#pragma once

#include <cmath>
#include <limits>
#include <string>

#include "types.hpp"

namespace opossum {

/**
 * HistogramDomain<T> is a template specialized for integral types, floating point types, and strings respectively.
 * It provides a function `next_value_clamped()` for all three type categories and `previous_value_clamped()`
 * for arithmetic types.
 * The functions are "clamped" simply because, e.g. there is no next value for INT_MAX in the domain of integers. As
 * histograms are only used for estimations (and not for pruning decisions), this is acceptable.
 *
 * This enables algorithms working on histograms to be written more generically than having to specialize templates all
 * over the place.
 *
 * The reason this is a class template, as opposed to more simple functions, is that the
 * HistogramDomain<std::string> has a state (a character range and a prefix length)
 */

template <typename T, typename Enable = void>
class HistogramDomain {};

template <typename T>
class HistogramDomain<T, std::enable_if_t<std::is_integral_v<T>>> {
 public:
  T next_value_clamped(T v) const {
    if (v == std::numeric_limits<T>::max()) return v;
    return v + 1;
  }

  T previous_value_clamped(T v) const {
    if (v == std::numeric_limits<T>::min()) return v;
    return v - 1;
  }
};

template <typename T>
class HistogramDomain<T, std::enable_if_t<std::is_floating_point_v<T>>> {
 public:
  T next_value_clamped(T v) const { return std::nextafter(v, std::numeric_limits<T>::infinity()); }

  T previous_value_clamped(T v) const { return std::nextafter(v, -std::numeric_limits<T>::infinity()); }
};

template <>
class HistogramDomain<pmr_string> {
 public:
  using IntegralType = uint64_t;

  // Use default character set and prefix length
  HistogramDomain();

  /**
   * @param init_min_char        The minimum of the supported character range
   * @param init_max_char        The maximum of the supported character range
   * @param init_prefix_length
   */
  HistogramDomain(const char init_min_char, const char init_max_char, const size_t init_prefix_length);

  /**
   * @return whether @param string_value consists exclusively of characters between `min_char` and `max_char`
   */
  bool contains(const pmr_string& string_value) const;

  /**
   * @return max_char - min_char + 1
   */
  size_t character_range_width() const;

  /**
   * @return a numerical representation of @param string_value. Note that only the first `prefix_length` characters are
   *         considered and that each character of @param string_value is capped by [min_char, max_char]
   */
  IntegralType string_to_number(const pmr_string& string_value) const;

  /**
   * @return a copy of @param string_value with all characters capped by [min_char, max_char]
   */
  pmr_string string_to_domain(const pmr_string& string_value) const;

  /**
   * @param string_in_domain    A string for which contains(string_in_domain) holds
   * @return                    The string is capped to prefix_length and then the next lexicographically larger string
   *                            is computed. If @param string_in_domain is the greatest string representable within this
   *                            domain, `next_value(v) == v`
   *
   */
  pmr_string next_value_clamped(const pmr_string& string_in_domain) const;

  bool operator==(const HistogramDomain& rhs) const;

  char min_char{};
  char max_char{};
  size_t prefix_length;

 private:
  // For a prefix length of 4 and a character range width of 26 this returns `26^3 + 26^2 + 26^1 + 26^0`. This is the
  // base number used for transforming strings into integral values. The base number is the "weight" of the first
  // character in the string. The second character has the weight `26^2 + 26^1 + 26^0` and so forth.
  IntegralType _base_number() const;
};

using StringHistogramDomain = HistogramDomain<pmr_string>;

/**
 * Returns the power of base to exp. Only required for StringHistogramDomain and tests, so declared here.
 * The standard library function works on doubles and is inaccurate for large numbers.
 */
uint64_t ipow(uint64_t base, uint64_t exp);

}  // namespace opossum
