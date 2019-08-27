#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/table.hpp"

namespace opossum {

class TableVerificationTest : public BaseTest {};

TEST_F(TableVerificationTest, CaseInsensitiveColumns) {
  const auto table_aa =
      std::make_shared<Table>(TableColumnDefinitions{{"aa", DataType::Float, false}}, TableType::Data);
  const auto table_aa_1 =
      std::make_shared<Table>(TableColumnDefinitions{{"aa", DataType::Float, false}}, TableType::Data);
  const auto table_aA =
      std::make_shared<Table>(TableColumnDefinitions{{"aA", DataType::Float, false}}, TableType::Data);
  const auto table_Aa =
      std::make_shared<Table>(TableColumnDefinitions{{"Aa", DataType::Float, false}}, TableType::Data);
  const auto table_AA =
      std::make_shared<Table>(TableColumnDefinitions{{"AA", DataType::Float, false}}, TableType::Data);
  const auto table_ab =
      std::make_shared<Table>(TableColumnDefinitions{{"ab", DataType::Float, false}}, TableType::Data);

  EXPECT_EQ(check_table_equal(table_aa, table_aa_1, OrderSensitivity::No, TypeCmpMode::Strict,
                              FloatComparisonMode::AbsoluteDifference, IgnoreNullable::No),
            std::nullopt);
  EXPECT_EQ(check_table_equal(table_aa, table_aA, OrderSensitivity::No, TypeCmpMode::Strict,
                              FloatComparisonMode::AbsoluteDifference, IgnoreNullable::No),
            std::nullopt);
  EXPECT_EQ(check_table_equal(table_aa, table_Aa, OrderSensitivity::No, TypeCmpMode::Strict,
                              FloatComparisonMode::AbsoluteDifference, IgnoreNullable::No),
            std::nullopt);
  EXPECT_EQ(check_table_equal(table_aa, table_AA, OrderSensitivity::No, TypeCmpMode::Strict,
                              FloatComparisonMode::AbsoluteDifference, IgnoreNullable::No),
            std::nullopt);
  EXPECT_NE(check_table_equal(table_aa, table_ab, OrderSensitivity::No, TypeCmpMode::Strict,
                              FloatComparisonMode::AbsoluteDifference, IgnoreNullable::No),
            std::nullopt);
}

TEST_F(TableVerificationTest, NullableColumns) {
  const auto table_nullable =
      std::make_shared<Table>(TableColumnDefinitions{{"aa", DataType::Float, true}}, TableType::Data);
  const auto table_not_nullable =
      std::make_shared<Table>(TableColumnDefinitions{{"aa", DataType::Float, false}}, TableType::Data);

  EXPECT_NE(check_table_equal(table_nullable, table_not_nullable, OrderSensitivity::No, TypeCmpMode::Strict,
                              FloatComparisonMode::AbsoluteDifference, IgnoreNullable::No),
            std::nullopt);
}

}  // namespace opossum
