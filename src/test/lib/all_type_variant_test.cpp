#include <cstdlib>
#include <string>

#include <boost/variant.hpp>

#include "base_test.hpp"

#include "types.hpp"

namespace opossum {

template <typename T>
class AllTypeVariantTest : public BaseTest {};

using AllTypeVariantTestDataTypes = ::testing::Types<int32_t, int64_t, float, double, pmr_string>;
TYPED_TEST_SUITE(AllTypeVariantTest, AllTypeVariantTestDataTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(AllTypeVariantTest, GetExtractsExactNumericalValue) {
  auto values = std::vector<TypeParam>{};
  if constexpr (std::is_same_v<TypeParam, pmr_string>) {
    values.emplace_back(pmr_string{});
    values.emplace_back(pmr_string{"shortstring"});
    values.emplace_back(pmr_string{"reallyreallylongstringthatcantbestoredusingsso"});
  } else {
    values.emplace_back(std::numeric_limits<TypeParam>::min());
    values.emplace_back(std::numeric_limits<TypeParam>::lowest());
    values.emplace_back(std::numeric_limits<TypeParam>::max());
    values.emplace_back(TypeParam{0});
    values.emplace_back(TypeParam{17});
  }

  for (auto value_in : values) {
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = boost::get<TypeParam>(variant);

    ASSERT_EQ(value_in, value_out);
  }
}

}  // namespace opossum
