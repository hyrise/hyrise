#include <cstdlib>
#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/types.hpp"

namespace opossum {

class AllTypeVariantTest : public BaseTest {};

TEST_F(AllTypeVariantTest, TypeCastExtractsCorrectValue) {
  const auto float_value_in = static_cast<float>(std::rand()) / RAND_MAX;
  const auto float_variant = AllTypeVariant{float_value_in};
  const auto float_value_out = type_cast<float>(float_variant);

  ASSERT_FLOAT_EQ(float_value_in, float_value_out);
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
