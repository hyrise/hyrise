#include "base_test.hpp"
#include "operators/jit_operator/jit_types.hpp"

namespace opossum {

class JitVariantVectorTest : public BaseTest {};

TEST_F(JitVariantVectorTest, GetAndSet) {
  JitVariantVector vector;
  vector.resize(10);

  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto value_in = static_cast<int32_t>(std::rand());
    vector.set<int32_t>(index, value_in);
    const auto value_out = vector.get<int32_t>(index);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto value_in = static_cast<int64_t>(std::rand());
    vector.set<int64_t>(index, value_in);
    const auto value_out = vector.get<int64_t>(index);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto value_in = static_cast<float>(std::rand()) / RAND_MAX;
    vector.set<float>(index, value_in);
    const auto value_out = vector.get<float>(index);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto value_in = static_cast<double>(std::rand()) / RAND_MAX;
    vector.set<double>(index, value_in);
    const auto value_out = vector.get<double>(index);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto value_in = pmr_string("some string");
    vector.set<pmr_string>(index, value_in);
    const auto value_out = vector.get<pmr_string>(index);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<int32_t>(std::rand()) % 10;
    const auto value_in = false;
    vector.set<bool>(index, value_in);
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

TEST_F(JitVariantVectorTest, GetVector) {
  const auto vector_size = 10u;
  JitVariantVector vector;
  vector.resize(vector_size);

  {
    const auto value = 1234.5f;
    const auto index = 2;
    vector.set<float>(index, value);
    EXPECT_EQ(vector.get_vector<float>().size(), vector_size);
    EXPECT_EQ(vector.get_vector<float>()[index], value);
  }
  {
    const auto value = 1234;
    const auto index = 4;
    vector.set<int32_t>(index, value);
    EXPECT_EQ(vector.get_vector<int32_t>().size(), vector_size);
    EXPECT_EQ(vector.get_vector<int32_t>()[index], value);
  }
  {
    const auto value = pmr_string("1234");
    const auto index = 6;
    vector.set<pmr_string>(index, value);
    EXPECT_EQ(vector.get_vector<pmr_string>().size(), vector_size);
    EXPECT_EQ(vector.get_vector<pmr_string>()[index], value);
  }
}

TEST_F(JitVariantVectorTest, GrowByOneAddsElementToCorrectInternalVector) {
  {
    JitVariantVector vector;
    vector.grow_by_one<bool>(JitVariantVector::InitialValue::Zero);
    EXPECT_EQ(vector.get_vector<bool>().size(), 1u);
    EXPECT_EQ(vector.get_vector<int32_t>().size(), 0u);
    EXPECT_EQ(vector.get_vector<int64_t>().size(), 0u);
    EXPECT_EQ(vector.get_vector<float>().size(), 0u);
    EXPECT_EQ(vector.get_vector<double>().size(), 0u);
    EXPECT_EQ(vector.get_vector<pmr_string>().size(), 0u);
  }
  {
    JitVariantVector vector;
    vector.grow_by_one<double>(JitVariantVector::InitialValue::Zero);
    EXPECT_EQ(vector.get_vector<bool>().size(), 0u);
    EXPECT_EQ(vector.get_vector<int32_t>().size(), 0u);
    EXPECT_EQ(vector.get_vector<int64_t>().size(), 0u);
    EXPECT_EQ(vector.get_vector<float>().size(), 0u);
    EXPECT_EQ(vector.get_vector<double>().size(), 1u);
    EXPECT_EQ(vector.get_vector<pmr_string>().size(), 0u);
  }
  {
    JitVariantVector vector;
    vector.grow_by_one<pmr_string>(JitVariantVector::InitialValue::Zero);
    EXPECT_EQ(vector.get_vector<bool>().size(), 0u);
    EXPECT_EQ(vector.get_vector<int32_t>().size(), 0u);
    EXPECT_EQ(vector.get_vector<int64_t>().size(), 0u);
    EXPECT_EQ(vector.get_vector<float>().size(), 0u);
    EXPECT_EQ(vector.get_vector<double>().size(), 0u);
    EXPECT_EQ(vector.get_vector<pmr_string>().size(), 1u);
  }
}

TEST_F(JitVariantVectorTest, GrowByOneAddsMultipleElements) {
  JitVariantVector vector;
  for (auto i = 1u; i <= 100u; ++i) {
    vector.grow_by_one<int32_t>(JitVariantVector::InitialValue::Zero);
    EXPECT_EQ(vector.get_vector<int32_t>().size(), i);
  }
}

TEST_F(JitVariantVectorTest, GrowByOneProperlyInitializesValues) {
  {
    JitVariantVector vector;
    vector.grow_by_one<int32_t>(JitVariantVector::InitialValue::Zero);
    vector.grow_by_one<int32_t>(JitVariantVector::InitialValue::MaxValue);
    vector.grow_by_one<int32_t>(JitVariantVector::InitialValue::MinValue);
    const auto& int_vector = vector.get_vector<int32_t>();
    EXPECT_EQ(int_vector.size(), 3u);
    EXPECT_EQ(int_vector[0], 0);
    EXPECT_EQ(int_vector[1], std::numeric_limits<int32_t>::max());
    EXPECT_EQ(int_vector[2], std::numeric_limits<int32_t>::min());
  }
  {
    JitVariantVector vector;
    vector.grow_by_one<float>(JitVariantVector::InitialValue::Zero);
    vector.grow_by_one<float>(JitVariantVector::InitialValue::MaxValue);
    vector.grow_by_one<float>(JitVariantVector::InitialValue::MinValue);
    const auto& double_vector = vector.get_vector<float>();
    EXPECT_EQ(double_vector.size(), 3u);
    EXPECT_EQ(double_vector[0], 0.0f);
    EXPECT_EQ(double_vector[1], std::numeric_limits<float>::max());
    EXPECT_EQ(double_vector[2], std::numeric_limits<float>::min());
  }
  {
    JitVariantVector vector;
    vector.grow_by_one<double>(JitVariantVector::InitialValue::Zero);
    vector.grow_by_one<double>(JitVariantVector::InitialValue::MaxValue);
    vector.grow_by_one<double>(JitVariantVector::InitialValue::MinValue);
    const auto& double_vector = vector.get_vector<double>();
    EXPECT_EQ(double_vector.size(), 3u);
    EXPECT_EQ(double_vector[0], 0.0);
    EXPECT_EQ(double_vector[1], std::numeric_limits<double>::max());
    EXPECT_EQ(double_vector[2], std::numeric_limits<double>::min());
  }
}

}  // namespace opossum
