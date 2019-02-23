#include "base_test.hpp"
#include "operators/jit_operator/jit_types.hpp"

namespace opossum {

class JitHashmapValueTest : public BaseTest {};

TEST_F(JitHashmapValueTest, Accessors) {
  {
    JitHashmapValue hashmap_value{DataType::String, false, 123u};
    ASSERT_EQ(hashmap_value.data_type(), DataType::String);
    ASSERT_EQ(hashmap_value.is_nullable(), false);
    ASSERT_EQ(hashmap_value.column_index(), 123u);
  }
  {
    JitHashmapValue hashmap_value{DataType::Int, true, 456u};
    ASSERT_EQ(hashmap_value.data_type(), DataType::Int);
    ASSERT_EQ(hashmap_value.is_nullable(), true);
    ASSERT_EQ(hashmap_value.column_index(), 456u);
  }
  {
    JitHashmapValue hashmap_value{DataType::Double, true, 789u};
    ASSERT_EQ(hashmap_value.data_type(), DataType::Double);
    ASSERT_EQ(hashmap_value.is_nullable(), true);
    ASSERT_EQ(hashmap_value.column_index(), 789u);
  }
}

TEST_F(JitHashmapValueTest, GetAndSet) {
  JitRuntimeContext context;
  // Create a hashmap data structure with three columns with ten elements each
  context.hashmap.columns.resize(3);
  context.hashmap.columns[0].resize(10);
  context.hashmap.columns[1].resize(10);
  context.hashmap.columns[2].resize(10);

  // Perform the following test for each Hyrise data type
  const auto typed_test = [&](const DataType data_type, auto value_in) {
    using ValueType = decltype(value_in);
    // Set and get a value at a random location
    const auto column_index = static_cast<size_t>(std::rand()) % 3;
    const auto row_index = static_cast<size_t>(std::rand()) % 10;
    JitHashmapValue hashmap_value{data_type, false, column_index};
    hashmap_value.set<ValueType>(value_in, row_index, context);
    const auto value_out = hashmap_value.get<ValueType>(row_index, context);
    EXPECT_EQ(value_in, value_out);
  };

  typed_test(DataType::Int, static_cast<int32_t>(std::rand()));
  typed_test(DataType::Long, static_cast<int64_t>(std::rand()));
  typed_test(DataType::Float, static_cast<float>(std::rand()));
  typed_test(DataType::Double, static_cast<double>(std::rand()));
  typed_test(DataType::String, pmr_string("some string"));
}

TEST_F(JitHashmapValueTest, IsNullAndSetIsNull) {
  JitRuntimeContext context;
  context.hashmap.columns.resize(3);
  context.hashmap.columns[0].resize(10);
  context.hashmap.columns[1].resize(10);
  context.hashmap.columns[2].resize(10);

  {
    const auto column_index = static_cast<size_t>(std::rand()) % 3;
    const auto row_index = static_cast<size_t>(std::rand()) % 10;
    JitHashmapValue hashmap_value{DataType::Int, true, column_index};
    const auto is_null_in = false;
    hashmap_value.set_is_null(is_null_in, row_index, context);
    const auto is_null_out = hashmap_value.is_null(row_index, context);
    EXPECT_EQ(is_null_in, is_null_out);
  }
  {
    const auto column_index = static_cast<size_t>(std::rand()) % 3;
    const auto row_index = static_cast<size_t>(std::rand()) % 10;
    JitHashmapValue hashmap_value{DataType::Int, true, column_index};
    const auto is_null_in = true;
    hashmap_value.set_is_null(is_null_in, row_index, context);
    const auto is_null_out = hashmap_value.is_null(row_index, context);
    EXPECT_EQ(is_null_in, is_null_out);
  }
}

}  // namespace opossum
