#include <cstdint>
#include <string>

#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "types.hpp"

namespace hyrise {

class AllParameterVariantTest : public BaseTest {};

TEST_F(AllParameterVariantTest, GetCurrentType) {
  {
    const auto parameter = AllParameterVariant{ColumnID{0}};
    EXPECT_EQ(parameter.type(), typeid(ColumnID));
    EXPECT_NE(parameter.type(), typeid(AllTypeVariant));
  }
  {
    const auto parameter = AllParameterVariant{"string"};
    EXPECT_NE(parameter.type(), typeid(ColumnID));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    const auto parameter = AllParameterVariant{true};
    EXPECT_NE(parameter.type(), typeid(ColumnID));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    const auto parameter = AllParameterVariant{static_cast<int32_t>(123)};
    EXPECT_NE(parameter.type(), typeid(ColumnID));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    const auto parameter = AllParameterVariant{static_cast<int64_t>(123456789l)};
    EXPECT_NE(parameter.type(), typeid(ColumnID));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    const auto parameter = AllParameterVariant{123.4f};
    EXPECT_NE(parameter.type(), typeid(ColumnID));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    const auto parameter = AllParameterVariant{123.4};
    EXPECT_NE(parameter.type(), typeid(ColumnID));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
}

TEST_F(AllParameterVariantTest, GetCurrentValue) {
  {
    const auto parameter = AllParameterVariant{ColumnID{0}};
    EXPECT_EQ(static_cast<uint16_t>(boost::get<ColumnID>(parameter)), static_cast<uint16_t>(0u));
  }
  {
    const auto parameter = AllParameterVariant{"string"};
    const auto value = boost::get<pmr_string>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, "string");
  }
  {
    const auto parameter = AllParameterVariant{static_cast<int32_t>(123)};
    const auto value = boost::get<int32_t>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, static_cast<int32_t>(123));
  }
  {
    const auto parameter = AllParameterVariant{static_cast<int64_t>(123456789l)};
    const auto value = boost::get<int64_t>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, static_cast<int64_t>(123456789l));
  }
  {
    const auto parameter = AllParameterVariant{123.4f};
    const auto value = boost::get<float>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, 123.4f);
  }
  {
    const auto parameter = AllParameterVariant{123.4};
    const auto value = boost::get<double>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, 123.4);
  }
}

TEST_F(AllParameterVariantTest, OutputToStream) {
  {
    const auto parameter = AllParameterVariant{ParameterID{17}};
    auto stream = std::ostringstream{};
    stream << parameter;
    EXPECT_EQ(stream.str(), "Placeholder #17");
  }
  {
    const auto parameter = AllParameterVariant{ColumnID{17}};
    auto stream = std::ostringstream{};
    stream << parameter;
    EXPECT_EQ(stream.str(), "Column #17");
  }
  {
    const auto parameter = AllParameterVariant{"string"};
    auto stream = std::ostringstream{};
    stream << parameter;
    EXPECT_EQ(stream.str(), "string");
  }
}

}  // namespace hyrise
