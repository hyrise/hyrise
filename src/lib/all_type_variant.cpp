#include "all_type_variant.hpp"

#include <cmath>

namespace opossum {

bool all_type_variant_near(const AllTypeVariant& lhs, const AllTypeVariant& rhs) {
  constexpr auto max_abs_error = 0.001;

  if (lhs.type() == typeid(float) && rhs.type() == typeid(float)) {
    return std::fabs(boost::get<float>(lhs) - boost::get<float>(rhs)) < max_abs_error;
  } else if (lhs.type() == typeid(double) && rhs.type() == typeid(double)) {
    return std::fabs(boost::get<double>(lhs) - boost::get<double>(rhs)) < max_abs_error;
  }

  return lhs == rhs;
}

}  // namespace opossum