#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"

#include "all_parameter_variant.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

class AllParameterVariantTest : public BaseTest {};

TEST_F(AllParameterVariantTest, GetCurrentType) {
  {
    AllParameterVariant parameter(ColumnID{0});
    EXPECT_EQ(parameter.type(), typeid(ColumnID));
    EXPECT_NE(parameter.type(), typeid(AllTypeVariant));
  }
  {
    AllParameterVariant parameter("string");
    EXPECT_NE(parameter.type(), typeid(ColumnID));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    AllParameterVariant parameter(true);
    EXPECT_NE(parameter.type(), typeid(ColumnID));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    AllParameterVariant parameter(static_cast<int32_t>(123));
    EXPECT_NE(parameter.type(), typeid(ColumnID));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    AllParameterVariant parameter(static_cast<int64_t>(123456789l));
    EXPECT_NE(parameter.type(), typeid(ColumnID));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    AllParameterVariant parameter(123.4f);
    EXPECT_NE(parameter.type(), typeid(ColumnID));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    AllParameterVariant parameter(123.4);
    EXPECT_NE(parameter.type(), typeid(ColumnID));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
}

TEST_F(AllParameterVariantTest, GetCurrentValue) {
  {
    AllParameterVariant parameter(ColumnID{0});
    EXPECT_EQ(static_cast<std::uint16_t>(boost::get<ColumnID>(parameter)), static_cast<std::uint16_t>(0u));
  }
  {
    AllParameterVariant parameter("string");
    auto value = type_cast_variant<std::string>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, "string");
  }
  {
    AllParameterVariant parameter(static_cast<int32_t>(123));
    auto value = type_cast_variant<int32_t>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, static_cast<int32_t>(123));
  }
  {
    AllParameterVariant parameter(static_cast<int64_t>(123456789l));
    auto value = type_cast_variant<int64_t>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, static_cast<int64_t>(123456789l));
  }
  {
    AllParameterVariant parameter(123.4f);
    auto value = type_cast_variant<float>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, 123.4f);
  }
  {
    AllParameterVariant parameter(123.4);
    auto value = type_cast_variant<double>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, 123.4);
  }
}

TEST_F(AllParameterVariantTest, ToString) {
  {
    const AllParameterVariant parameter(ParameterID{17});
    EXPECT_EQ(to_string(parameter), "Placeholder #17");
  }
  {
    const AllParameterVariant parameter(ColumnID{17});
    EXPECT_EQ(to_string(parameter), "Column #17");
  }
  {
    StorageManager::get().add_table("int_float", load_table("src/test/tables/int_float.tbl"));
    const std::shared_ptr<StoredTableNode> int_float = StoredTableNode::make("int_float");
    const LQPColumnReference column_a = {int_float, ColumnID{0}};
    const LQPColumnReference column_b = {int_float, ColumnID{1}};

    const AllParameterVariant parameter_column_a(column_a);
    EXPECT_EQ(to_string(parameter_column_a), "a");

    const AllParameterVariant parameter_column_b(column_b);
    EXPECT_EQ(to_string(parameter_column_b), "b");
  }
  {
    const AllParameterVariant parameter("string");
    EXPECT_EQ(to_string(parameter), "string");
  }
}

}  // namespace opossum
