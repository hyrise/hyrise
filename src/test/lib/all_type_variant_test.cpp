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

TEST_F(AllTypeVariantTest, ValuesNear) {
  {
    const auto variant_0 = AllTypeVariant{1.0f};
    const auto variant_1 = AllTypeVariant{1.1f};

    ASSERT_TRUE(all_type_variant_near(variant_0, variant_0, 0.00001));

    ASSERT_TRUE(all_type_variant_near(variant_0, variant_1, 0.2));
    ASSERT_FALSE(all_type_variant_near(variant_0, variant_1, 0.05));
  }
  {
    const auto variant_0 = AllTypeVariant{1.0};
    const auto variant_1 = AllTypeVariant{1.1};

    ASSERT_TRUE(all_type_variant_near(variant_0, variant_0, 0.00001));

    ASSERT_TRUE(all_type_variant_near(variant_0, variant_1, 0.2));
    ASSERT_FALSE(all_type_variant_near(variant_0, variant_1, 0.05));
  }
  {
    const auto variant_0 = AllTypeVariant{1.0f};
    const auto variant_1 = AllTypeVariant{1.1};

    ASSERT_TRUE(all_type_variant_near(variant_0, variant_1, 0.2));
    ASSERT_FALSE(all_type_variant_near(variant_0, variant_1, 0.05));

    ASSERT_TRUE(all_type_variant_near(variant_1, variant_0, 0.2));
    ASSERT_FALSE(all_type_variant_near(variant_1, variant_0, 0.05));
  }
}

}  // namespace opossum
