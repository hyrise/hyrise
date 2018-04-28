#include "../../base_test.hpp"
#include "operators/jit_operator/jit_types.hpp"

namespace opossum {

class JitVariantVectorTest : public BaseTest {};

TEST_F(JitVariantVectorTest, GetAndSet) {
  JitVariantVector vector;
  vector.resize(10);

  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto value_in = static_cast<int32_t>(std::rand());
    vector.set(index, value_in);
    const auto value_out = vector.get<int32_t>(index);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto value_in = static_cast<int64_t>(std::rand());
    vector.set(index, value_in);
    const auto value_out = vector.get<int64_t>(index);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto value_in = static_cast<float>(std::rand()) / RAND_MAX;
    vector.set(index, value_in);
    const auto value_out = vector.get<float>(index);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto value_in = static_cast<double>(std::rand()) / RAND_MAX;
    vector.set(index, value_in);
    const auto value_out = vector.get<double>(index);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto value_in = std::string("some string");
    vector.set(index, value_in);
    const auto value_out = vector.get<std::string>(index);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto value_in = false;
    vector.set(index, value_in);
    const auto value_out = vector.get<bool>(index);
    EXPECT_EQ(value_in, value_out);
  }
}

TEST_F(JitVariantVectorTest, IsNullAndSetIsNull) {
  JitVariantVector vector;
  vector.resize(10);

  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto is_null_in = false;
    vector.set_is_null(index, is_null_in);
    const auto is_null_out = vector.is_null(index);
    EXPECT_EQ(is_null_in, is_null_out);
  }
  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto is_null_in = true;
    vector.set_is_null(index, is_null_in);
    const auto is_null_out = vector.is_null(index);
    EXPECT_EQ(is_null_in, is_null_out);
  }
}

}  // namespace opossum
