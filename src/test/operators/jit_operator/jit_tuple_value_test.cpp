#include "base_test.hpp"
#include "operators/jit_operator/jit_types.hpp"

namespace opossum {

class JitTupleValueTest : public BaseTest {};

TEST_F(JitTupleValueTest, GetAndSet) {
  JitRuntimeContext context;
  context.tuple.resize(10);

  {
    const auto index = static_cast<size_t>(std::rand()) % 10;
    JitTupleValue tuple_value{DataType::Int, false, index};
    const auto value_in = static_cast<int32_t>(std::rand());
    tuple_value.set(value_in, context);
    const auto value_out = tuple_value.get<int32_t>(context);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<size_t>(std::rand()) % 10;
    JitTupleValue tuple_value{DataType::Long, false, index};
    const auto value_in = static_cast<int64_t>(std::rand());
    tuple_value.set(value_in, context);
    const auto value_out = tuple_value.get<int64_t>(context);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<size_t>(std::rand()) % 10;
    JitTupleValue tuple_value{DataType::Float, false, index};
    const auto value_in = static_cast<float>(std::rand());
    tuple_value.set(value_in, context);
    const auto value_out = tuple_value.get<float>(context);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<size_t>(std::rand()) % 10;
    JitTupleValue tuple_value{DataType::Double, false, index};
    const auto value_in = static_cast<double>(std::rand());
    tuple_value.set(value_in, context);
    const auto value_out = tuple_value.get<double>(context);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<size_t>(std::rand()) % 10;
    JitTupleValue tuple_value{DataType::String, false, index};
    const auto value_in = pmr_string("some string");
    tuple_value.set(value_in, context);
    const auto value_out = tuple_value.get<pmr_string>(context);
    EXPECT_EQ(value_in, value_out);
  }
  {
    const auto index = static_cast<size_t>(std::rand()) % 10;
    JitTupleValue tuple_value{DataType::Bool, false, index};
    const auto value_in = false;
    tuple_value.set(value_in, context);
    const auto value_out = tuple_value.get<bool>(context);
    EXPECT_EQ(value_in, value_out);
  }
}

TEST_F(JitTupleValueTest, IsNullAndSetIsNull) {
  JitRuntimeContext context;
  context.tuple.resize(10);

  {
    const auto index = static_cast<size_t>(std::rand()) % 10;
    JitTupleValue tuple_value{DataType::Int, true, index};
    const auto is_null_in = false;
    tuple_value.set_is_null(is_null_in, context);
    const auto is_null_out = tuple_value.is_null(context);
    EXPECT_EQ(is_null_in, is_null_out);
  }
  {
    const auto index = static_cast<size_t>(std::rand()) % 10;
    JitTupleValue tuple_value{DataType::Int, true, index};
    const auto is_null_in = true;
    tuple_value.set_is_null(is_null_in, context);
    const auto is_null_out = tuple_value.is_null(context);
    EXPECT_EQ(is_null_in, is_null_out);
  }
}

}  // namespace opossum
