#include "../../base_test.hpp"
#include "operators/jit_operator/jit_types.hpp"

namespace opossum {

class JitHashmapValueTest : public BaseTest {};

TEST_F(JitHashmapValueTest, Accessors) {
  {
    JitHashmapValue hashmap_value{DataType::String, false, 123};
    ASSERT_EQ(hashmap_value.data_type(), DataType::String);
    ASSERT_EQ(hashmap_value.is_nullable(), false);
    ASSERT_EQ(hashmap_value.column_index(), 123);
  }
  {
    JitHashmapValue hashmap_value{DataType::Int, true, 456};
    ASSERT_EQ(hashmap_value.data_type(), DataType::Int);
    ASSERT_EQ(hashmap_value.is_nullable(), false);
    ASSERT_EQ(hashmap_value.column_index(), 456);
  }
  {
    JitHashmapValue hashmap_value{DataType::Double, true, 789};
    ASSERT_EQ(hashmap_value.data_type(), DataType::Double);
    ASSERT_EQ(hashmap_value.is_nullable(), true);
    ASSERT_EQ(hashmap_value.column_index(), 789);
  }
}

TEST_F(JitHashMapValueTest, GetAndSet) {
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
    const auto value_in = std::string("some string");
    tuple_value.set(value_in, context);
    const auto value_out = tuple_value.get<std::string>(context);
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

class JitHashmapValue {
 public:
  template <typename T>
  T get(const size_t index, JitRuntimeContext& context) const {
    return context.hashmap.values[_column_index].get<T>(index);
  }
  template <typename T>
  void set(const T value, const size_t index, JitRuntimeContext& context) const {
    context.hashmap.values[_column_index].set<T>(index, value);
  }
  inline bool is_null(const size_t index, JitRuntimeContext& context) const {
    return _is_nullable && context.hashmap.values[_column_index].is_null(index);
  }
  inline void set_is_null(const bool is_null, const size_t index, JitRuntimeContext& context) const {
    context.hashmap.values[_column_index].set_is_null(index, is_null);
  }

 private:
  const DataType _data_type;
  const bool _is_nullable;
  const size_t _column_index;
};
