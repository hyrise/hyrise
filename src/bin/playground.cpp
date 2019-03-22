#include <iostream>
#include <optional>

#include "boost/numeric/conversion/converter.hpp"

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "type_cast.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

//template<typename T>
//std::optional<T> type_cast_variant_safe(const AllTypeVariant& variant) {
//  std::optional<T> result;
//
//  resolve_data_type(data_type_from_all_type_variant(variant), [&](const auto data_type_t) {
//    using VariantDataType = typename decltype(data_type_t)::type;
//
//    if constexpr (std::is_same_v<VariantDataType, NullValue>) {
//      result = std::nullopt;
//    } else if constexpr (std::is_same_v<VariantDataType, pmr_string> != std::is_same_v<T, pmr_string>) {
//      result = std::nullopt;
//    } else if constexpr (std::is_same_v<VariantDataType, pmr_string>)
//  });
//
//  return result;
//}

//template<typename T1, typename T2>
//void test(const T2& source) {
//  std::cout << source << " --> " << typeid(T1).name() << ": ";
//
//  const auto result = lossless_cast<T1>(source);
//  if (result) {
//    std::cout << *result << std::endl;
//  } else {
//    std::cout << "<inconvertible>" << std::endl;
//  }
//}

int main() {
  std::cout.setf(std::ios::fixed, std::ios::floatfield);
  std::cout.setf(std::ios::showpoint);

  std::cout << boost::numeric::converter<float, int32_t>::convert(20'000'003) << std::endl;
  std::cout << boost::numeric_cast<float>(20'000'003) << std::endl;

  return 0;
}
