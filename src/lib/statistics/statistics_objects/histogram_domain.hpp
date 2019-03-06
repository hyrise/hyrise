#pragma once

#include <cmath>
#include <limits>
#include <string>

#include "types.hpp"

namespace opossum {

/**
 * HistogramDomain<T> is a template specialized for ints, floats, and strings respectively. It provides a function
 * `next_value()` for all three type categories and `previous_value()` for ints and floats. This enables algorithms
 * working on histograms to be written more generically than having to specialize templates all over the place.
 *
 * The reason this is a class template, as opposed to more simple functions, is that the
 * HistogramDomain<std::string> has a state (a character range and a prefix length)
 */

template <typename T, typename Enable = void>
class HistogramDomain {};

template <typename T>
class HistogramDomain<T, std::enable_if_t<std::is_integral_v<T>>> {
 public:
  T next_value(T v) const { return v + 1; }

  T previous_value(T v) const { return v - 1; }
};

template <typename T>
class HistogramDomain<T, std::enable_if_t<std::is_floating_point_v<T>>> {
 public:
  T next_value(T v) const { return std::nextafter(v, std::numeric_limits<T>::infinity()); }

  T previous_value(T v) const { return std::nextafter(v, -std::numeric_limits<T>::infinity()); }
};

template <>
class HistogramDomain<pmr_string> {
 public:
  using IntegralType = uint64_t;

  // Use default character set and prefix length
  HistogramDomain();

  /**
   * @param min_char        The minimum of the supported character range
   * @param max_char        The maximum of the supported character range
   * @param prefix_length
   */
  HistogramDomain(const char min_char, const char max_char, const size_t prefix_length);

  /**
   * @return whether @param string_value consists exclusively of characters between `min_char` and `max_max`
   */
  bool contains(const pmr_string& string_value) const;

  /**
   * @return max_char - min_char + 1
   */
  size_t character_range_width() const;

  /**
   * @return a numerical representation of @param string_value. Not that only the first prefix_length are considered and
   *         that each character of @param string_value is capped by [min_char, max_char]
   */
  IntegralType string_to_number(const pmr_string& string_value) const;

  /**
   * @return a copy of @param string_value with all characters capped by [min_char, max_char]
   */
  pmr_string string_to_domain(const pmr_string& string_value) const;

  /**
   * @return the string is capped to prefix_length and then the next lexicographically larger string is computed. If
   *         @param string_value is the greatest string representable within this domain, `next_value(v) == v`
   */
  pmr_string next_value(const pmr_string& string_value) const;

  bool operator==(const HistogramDomain& rhs) const;

  char min_char{};
  char max_char{};
  size_t prefix_length;

 private:
  IntegralType _base_number() const;
};

using StringHistogramDomain = HistogramDomain<pmr_string>;

}  // namespace opossum
