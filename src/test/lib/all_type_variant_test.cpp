#include <cstdlib>
#include <string>

#include <boost/variant.hpp>

#include "base_test.hpp"

#include "types.hpp"

namespace opossum {

class AllTypeVariantTest : public BaseTest {};

TEST_F(AllTypeVariantTest, GetExtractsExactNumericalValue) {
  {
    const auto value_in = static_cast<float>(std::rand()) / RAND_MAX;
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = boost::get<float>(variant);

    ASSERT_EQ(value_in, value_out);
  }
  {
    const auto value_in = static_cast<double>(std::rand()) / RAND_MAX;
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = boost::get<double>(variant);

    ASSERT_EQ(value_in, value_out);
  }
  {
    const auto value_in = static_cast<int32_t>(std::rand());
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = boost::get<int32_t>(variant);

    ASSERT_EQ(value_in, value_out);
  }
  {
    const auto value_in = static_cast<int64_t>(std::rand());
    const auto variant = AllTypeVariant{value_in};
    const auto value_out = boost::get<int64_t>(variant);

    ASSERT_EQ(value_in, value_out);
  }
}

}  // namespace opossum
