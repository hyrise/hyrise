#pragma once

#include <string>

namespace opossum {

// To represent Strings as Histogram bounds they need to be converted to integrals for some purposes.
// A StringHistogramDomain implements this conversion using a prefix length and a supported character set.
// The character set is a continuous subset (from min_char to max_char) of the values that `char` can represent.
class StringHistogramDomain {
 public:
  using IntegralType = uint64_t;

  // Use default character set and prefix length
  StringHistogramDomain();

  /**
   * @param min_char        The minimum of the supported character range
   * @param max_char        The maximum of the supported character range
   * @param prefix_length
   */
  StringHistogramDomain(const char min_char, const char max_char, const size_t prefix_length);

  /**
   * @return whether @param string_value consists exclusively of characters between `min_char` and `max_max`
   */
  bool contains(const std::string& string_value) const;

  /**
   * @return contains(string_value) && string_value.size() <= prefix_length
   */
  bool is_valid_prefix(const std::string& string_value) const;

  /**
   * @return max_char - min_char + 1
   */
  size_t character_range_width() const;

  std::string number_to_string(IntegralType int_value) const;
  IntegralType string_to_number(const std::string& string_value) const;

  std::string string_to_domain(const std::string& string_value) const;

  std::string next_value(const std::string& string_value) const;

  bool operator==(const StringHistogramDomain& rhs) const;

  char min_char{};
  char max_char{};
  size_t prefix_length;

 private:
  IntegralType _base_number() const;
};

}  // namespace opossum