#include <cstdlib>
#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/type_cast.hpp"
#include "../lib/types.hpp"

namespace opossum {

class AllTypeVariantTest : public BaseTest {};

TEST_F(AllTypeVariantTest, TypeCastExtractsExactValue) {
  {
    const auto value_in = static_cast<float>(std::rand()) / RAND_MAX;
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = type_cast<float>(variant);

    ASSERT_EQ(value_in, value_out);
  }
  {
    const auto value_in = static_cast<double>(std::rand()) / RAND_MAX;
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = type_cast<double>(variant);

    ASSERT_EQ(value_in, value_out);
  }
  {
    const auto value_in = static_cast<int32_t>(std::rand());
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = type_cast<int32_t>(variant);

    ASSERT_EQ(value_in, value_out);
  }
  {
    const auto value_in = static_cast<int64_t>(std::rand());
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = type_cast<int64_t>(variant);

    ASSERT_EQ(value_in, value_out);
  }
}

TEST_F(AllTypeVariantTest, GetExtractsExactNumericalValue) {
  {
    const auto value_in = static_cast<float>(std::rand()) / RAND_MAX;
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = get<float>(variant);

    ASSERT_EQ(value_in, value_out);
  }
  {
    const auto value_in = static_cast<double>(std::rand()) / RAND_MAX;
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = get<double>(variant);

    ASSERT_EQ(value_in, value_out);
  }
  {
    const auto value_in = static_cast<int32_t>(std::rand());
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = get<int32_t>(variant);

    ASSERT_EQ(value_in, value_out);
  }
  {
    const auto value_in = static_cast<int64_t>(std::rand());
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = get<int64_t>(variant);

    ASSERT_EQ(value_in, value_out);
  }
}

TEST_F(AllTypeVariantTest, MapVariantWhichToType) {
  EXPECT_EQ(type_by_all_type_variant_which[AllTypeVariant((int32_t)0).which()], "int");
  EXPECT_EQ(type_by_all_type_variant_which[AllTypeVariant((int64_t)0).which()], "long");
  EXPECT_EQ(type_by_all_type_variant_which[AllTypeVariant((float)0).which()], "float");
  EXPECT_EQ(type_by_all_type_variant_which[AllTypeVariant((double)0).which()], "double");
  EXPECT_EQ(type_by_all_type_variant_which[AllTypeVariant(std::string()).which()], "string");
}

}  // namespace opossum
