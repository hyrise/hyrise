#include <memory>
#include <string_view>

#include "base_test.hpp"

#include "all_type_variant.hpp"
#include "null_value.hpp"

namespace opossum {

class NullValueTest : public BaseTest {};

TEST_F(NullValueTest, Comparators) {
  auto null_0 = NullValue{};
  auto null_1 = NullValue{};

  EXPECT_FALSE(null_0 == null_1);
  EXPECT_FALSE(null_0 != null_1);
  EXPECT_FALSE(null_0 < null_1);
  EXPECT_FALSE(null_0 <= null_1);
  EXPECT_FALSE(null_0 > null_1);
  EXPECT_FALSE(null_0 >= null_1);

  EXPECT_FALSE(-NullValue{} == NullValue{});
}

}  // namespace opossum
