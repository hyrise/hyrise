
#include "base_test.hpp" // NEEDEDINCLUDE


#include "all_parameter_variant.hpp" // NEEDEDINCLUDE
#include "type_cast.hpp" // NEEDEDINCLUDE

namespace opossum {

class AllParameterVariantTest : public BaseTest {};

TEST_F(AllParameterVariantTest, GetCurrentType) {
  {
    AllParameterVariant parameter(ColumnID{0});
    EXPECT_TRUE(std::holds_alternative<ColumnID>(parameter));
    EXPECT_FALSE(is_variant(parameter));
  }
  {
    AllParameterVariant parameter("string");
    EXPECT_FALSE(std::holds_alternative<ColumnID>(parameter));
    EXPECT_TRUE(is_variant(parameter));
  }
  {
    AllParameterVariant parameter(true);
    EXPECT_FALSE(std::holds_alternative<ColumnID>(parameter));
    EXPECT_TRUE(is_variant(parameter));
  }
  {
    AllParameterVariant parameter(static_cast<int32_t>(123));
    EXPECT_FALSE(std::holds_alternative<ColumnID>(parameter));
    EXPECT_TRUE(is_variant(parameter));
  }
  {
    AllParameterVariant parameter(static_cast<int64_t>(123456789l));
    EXPECT_FALSE(std::holds_alternative<ColumnID>(parameter));
    EXPECT_TRUE(is_variant(parameter));
  }
  {
    AllParameterVariant parameter(123.4f);
    EXPECT_FALSE(std::holds_alternative<ColumnID>(parameter));
    EXPECT_TRUE(is_variant(parameter));
  }
  {
    AllParameterVariant parameter(123.4);
    EXPECT_FALSE(std::holds_alternative<ColumnID>(parameter));
    EXPECT_TRUE(is_variant(parameter));
  }
}

TEST_F(AllParameterVariantTest, GetCurrentValue) {
  {
    AllParameterVariant parameter(ColumnID{0});
    EXPECT_EQ(static_cast<std::uint16_t>(std::get<ColumnID>(parameter)), static_cast<std::uint16_t>(0u));
  }
  {
    AllParameterVariant parameter("string");
    auto value = type_cast_variant<std::string>(to_all_type_variant(parameter));
    EXPECT_EQ(value, "string");
  }
  {
    AllParameterVariant parameter(static_cast<int32_t>(123));
    auto value = type_cast_variant<int32_t>(to_all_type_variant(parameter));
    EXPECT_EQ(value, static_cast<int32_t>(123));
  }
  {
    AllParameterVariant parameter(static_cast<int64_t>(123456789l));
    auto value = type_cast_variant<int64_t>(to_all_type_variant(parameter));
    EXPECT_EQ(value, static_cast<int64_t>(123456789l));
  }
  {
    AllParameterVariant parameter(123.4f);
    auto value = type_cast_variant<float>(to_all_type_variant(parameter));
    EXPECT_EQ(value, 123.4f);
  }
  {
    AllParameterVariant parameter(123.4);
    auto value = type_cast_variant<double>(to_all_type_variant(parameter));
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
    const AllParameterVariant parameter("string");
    EXPECT_EQ(to_string(parameter), "string");
  }
}

}  // namespace opossum
