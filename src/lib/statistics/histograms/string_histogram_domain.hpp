#pragma once

#include <string>

namespace opossum {

// To represent Strings as Histogram bounds they need to be converted to integrals.
// A StringHistogramDomain drives this conversion using a prefix length and a supported character set
class StringHistogramDomain {
 public:
  using IntegralType = uint64_t;

  static std::string string_before(std::string string_value, const std::string& lower_bound);

  // Use default character set and prefix length
  StringHistogramDomain();

  StringHistogramDomain(const std::string& supported_characters, const size_t prefix_length);

  std::string number_to_string(IntegralType int_value) const;
  IntegralType string_to_number(const std::string& string_value) const;

  std::string string_to_domain(const std::string& string_value) const;

  bool contains(const std::string& string_value) const;

  std::string next_value(const std::string& string_value) const;
  std::string previous_value(const std::string& string_value) const;

  IntegralType base_number() const;

  bool operator==(const StringHistogramDomain& rhs) const;

  std::string supported_characters;
  size_t prefix_length;
};


}  // namespace opossum