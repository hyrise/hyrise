#include <cstdlib>
#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

class AllTypeVariantTest : public BaseTest {};

TEST_F(AllTypeVariantTest, TypeCastExtractsExactValue) {
  {
    const auto value_in = static_cast<float>(std::rand()) / RAND_MAX;
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = type_cast_variant<float>(variant);

    ASSERT_EQ(value_in, value_out);
  }
  {
    const auto value_in = static_cast<double>(std::rand()) / RAND_MAX;
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = type_cast_variant<double>(variant);

    ASSERT_EQ(value_in, value_out);
  }
  {
    const auto value_in = static_cast<int32_t>(std::rand());
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = type_cast_variant<int32_t>(variant);

    ASSERT_EQ(value_in, value_out);
  }
  {
    const auto value_in = static_cast<int64_t>(std::rand());
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = type_cast_variant<int64_t>(variant);

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

}  // namespace opossum
