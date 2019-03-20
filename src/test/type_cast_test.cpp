#include "gtest/gtest.h"

#include "type_cast.hpp"

namespace opossum {

// Identity
template<typename T>
std::optional<T> type_cast_safe(const T& source) {
  return source;
}

// Integral to Floating Point
template<typename Target, typename Source>
std::enable_if_t<std::is_floating_point_v<Target> && std::is_integral_v<Source>, std::optional<Target>>
type_cast_safe(Source source) {
  auto f = static_cast<Target>(source);
  auto i = static_cast<Source>(f);
  if (source == i) {
    return f;
  } else {
    return std::nullopt;
  }
}

// Floating Point Type to Integral Type
template<typename Target, typename Source>
std::enable_if_t<std::is_integral_v<Target> && std::is_floating_point_v<Source>, std::optional<Target>>
type_cast_safe(const Source& source) {
  auto i = static_cast<Target>(source);
  auto f = static_cast<Source>(i);
  if (source == f) {
    return i;
  } else {
    return std::nullopt;
  }
}

// Integral Type to different Integral Type
template<typename Target, typename Source>
std::enable_if_t<std::is_integral_v<Target> && std::is_integral_v<Source> && !std::is_same_v<Target, Source>, std::optional<Target>>
type_cast_safe(const Source& source) {
  auto i0 = static_cast<Target>(source);
  auto i1 = static_cast<Source>(i0);
  if (source == i1) {
    return i0;
  } else {
    return std::nullopt;
  }
}

//
template<typename Target, typename Source>
std::optional<Target> type_cast_for_predicate(const Source& source, PredicateCondition predicate_condition) {



}

class TypeCastTest : public ::testing::Test{};

TEST_F(TypeCastTest, TypeCastSafeIdenticalSourceAndTarget) {
  EXPECT_EQ(type_cast_safe<int32_t>(int32_t(5)), int32_t(5));
  EXPECT_EQ(type_cast_safe<int64_t>(int64_t(5'000'000'000)), int64_t(5'000'000'000));
  EXPECT_EQ(type_cast_safe<float>(3.2f), 3.2f);
  EXPECT_EQ(type_cast_safe<double>(3.2), 3.2);
  EXPECT_EQ(type_cast_safe<pmr_string>("hello"), pmr_string("hello"));
}

TEST_F(TypeCastTest, TypeCastSafeIntegralToFloatingPoint) {
  EXPECT_EQ(type_cast_safe<float>(int32_t(3)), 3.0f);
  EXPECT_EQ(type_cast_safe<float>(int64_t(3)), 3.0f);
  EXPECT_EQ(type_cast_safe<double>(int32_t(3)), 3.0);
  EXPECT_EQ(type_cast_safe<double>(int64_t(3)), 3.0);

  EXPECT_EQ(type_cast_safe<float>(int32_t(20'000'003)), std::nullopt);
  EXPECT_EQ(type_cast_safe<float>(int64_t(20'000'003)), std::nullopt);
  EXPECT_EQ(type_cast_safe<double>(int32_t(20'000'003)), 20'000'003);
  EXPECT_EQ(type_cast_safe<double>(int64_t(20'000'003)), 20'000'003);
  
  EXPECT_EQ(type_cast_safe<float>(int32_t(-20'000'003)), std::nullopt);
  EXPECT_EQ(type_cast_safe<double>(int32_t(-20'000'003)), -20'000'003);

  EXPECT_EQ(type_cast_safe<float>(int64_t(20'123'456'789'012'345ll)), std::nullopt);
  EXPECT_EQ(type_cast_safe<double>(int64_t(20'123'456'789'012'345ll)), std::nullopt);
}

TEST_F(TypeCastTest, TypeCastSafeFloatingPointToIntegral) {
  EXPECT_EQ(type_cast_safe<int32_t>(3.0f), 3);
  EXPECT_EQ(type_cast_safe<int32_t>(3.0), 3);
  EXPECT_EQ(type_cast_safe<int64_t>(3.0f), 3);
  EXPECT_EQ(type_cast_safe<int64_t>(3.0), 3);

  EXPECT_EQ(type_cast_safe<int32_t>(5'000'000'000.0f), std::nullopt);
  EXPECT_EQ(type_cast_safe<int32_t>(5'000'000'000.0), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(5'000'000'000.0f), 5'000'000'000);
  EXPECT_EQ(type_cast_safe<int64_t>(5'000'000'000.0), 5'000'000'000);

  EXPECT_EQ(type_cast_safe<int32_t>(1.23e16f), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(1.23e16f), 12'300'000'356'728'832);

  EXPECT_EQ(type_cast_safe<int64_t>(1.0e32f), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(1.0e32), std::nullopt);

  EXPECT_EQ(type_cast_safe<int32_t>(3.1f), std::nullopt);
  EXPECT_EQ(type_cast_safe<int32_t>(3.1), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(3.1f), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(3.1), std::nullopt);
}

TEST_F(TypeCastTest, TypeCastSafeIntegralToIntegral) {
  EXPECT_EQ(type_cast_safe<int64_t>(int32_t(3)), 3);
  EXPECT_EQ(type_cast_safe<int32_t>(int64_t(3)), 3);

  EXPECT_EQ(type_cast_safe<int32_t>(int64_t(3'000'000'000)), std::nullopt);
}

}  // namespace opossum
