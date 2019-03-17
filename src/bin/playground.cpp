#include <iostream>
#include <optional>

#include "resolve_type.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

// Identity
template<typename T>
std::optional<T> type_cast_safe(const T& source) {
  return source;
}

// Floating Point Type to Integral Type
template<typename Target, typename Source>
std::enable_if_t<std::is_floating_point_v<Target> && std::is_integral_v<Source>, std::optional<Target>>
type_cast_safe(const Source& source) {
  return source;
}

// Integral Type to different Integral Type
template<typename Target, typename Source>
std::enable_if_t<std::is_integral_v<Target> && std::is_integral_v<Source> && !std::is_same_v<Target, Source>, std::optional<Target>>
type_cast_safe(const Source& source) {
  return source;
}

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

template<typename T1, typename T2>
void test(const T2& source) {
  std::cout << source << " --> " << typeid(T1).name() << ": ";

  const auto result = type_cast_safe<T1>(source);
  if (result) {
    std::cout << *result << std::endl;
  } else {
    std::cout << "<inconvertible>" << std::endl;
  }
}

int main() {
  test<float>(13);
  test<int64_t>(13);
  test<int32_t>(13);
  return 0;
}
