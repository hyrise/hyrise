#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "configuration/calibration_column_specification.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "query/calibration_query_generator_predicate.hpp"
#include "storage/encoding_type.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class CalibrationQueryGeneratorPredicatesTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& manager = StorageManager::get();
    manager.add_table("int_int", load_table("src/test/tables/int_int.tbl", 1u));
    manager.add_table("int_string_filtered", load_table("src/test/tables/int_string_filtered.tbl", 1u));
    manager.add_table("int_int_int", load_table("src/test/tables/int_int_int.tbl", 1u));
  }
};

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValueInt) {
  const auto filter_column =
      std::make_pair("a", CalibrationColumnSpecification{DataType::Int, "uniform", false, 1, EncodingType::Unencoded});

  const auto table = StoredTableNode::make("int_int");
  auto predicate = CalibrationQueryGeneratorPredicates::generate_predicate_column_value(table, filter_column);

  ASSERT_TRUE(predicate);
  // Column a contains only a single distinct values, which is by design 0
  EXPECT_EQ("a <= 0", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnColumn) {
  const auto filter_column =
      std::make_pair("a", CalibrationColumnSpecification{DataType::Int, "uniform", false, 1, EncodingType::Unencoded});

  const auto table = StoredTableNode::make("int_int");
  auto predicate = CalibrationQueryGeneratorPredicates::generate_predicate_column_column(table, filter_column);

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a <= b", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, Like) {
  const auto filter_column = std::make_pair(
      "b", CalibrationColumnSpecification{DataType::String, "uniform", false, 1, EncodingType::Unencoded});

  const auto table = StoredTableNode::make("int_string_filtered");
  auto predicate = CalibrationQueryGeneratorPredicates::generate_predicate_like(table, filter_column);

  ASSERT_TRUE(predicate);
  const std::string prefix("b LIKE");
  EXPECT_EQ(predicate->as_column_name().compare(0, prefix.size(), prefix), 0);
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, EquiOnStrings) {
  const auto filter_column = std::make_pair(
      "b", CalibrationColumnSpecification{DataType::String, "uniform", false, 1, EncodingType::Unencoded});

  const auto table = StoredTableNode::make("int_string_filtered");
  auto predicate = CalibrationQueryGeneratorPredicates::generate_predicate_equi_on_strings(table, filter_column);

  ASSERT_TRUE(predicate);
  EXPECT_EQ("b = 'A'", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, BetweenValueValue) {
  const auto filter_column =
      std::make_pair("a", CalibrationColumnSpecification{DataType::Int, "uniform", false, 1, EncodingType::Unencoded});

  const auto table = StoredTableNode::make("int_int_int");
  auto predicate = CalibrationQueryGeneratorPredicates::generate_predicate_between_value_value(table, filter_column);

  ASSERT_TRUE(predicate);
  // Column a contains only a single distinct values, which is by design 0
  EXPECT_EQ("a BETWEEN 0 AND 0", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, BetweenColumnColumn) {
  const auto filter_column =
      std::make_pair("a", CalibrationColumnSpecification{DataType::Int, "uniform", false, 1, EncodingType::Unencoded});

  const auto table = StoredTableNode::make("int_int_int");
  auto predicate = CalibrationQueryGeneratorPredicates::generate_predicate_between_column_column(table, filter_column);

  ASSERT_TRUE(predicate);
  const std::string prefix("a BETWEEN");
  EXPECT_EQ(predicate->as_column_name().compare(0, prefix.size(), prefix), 0);
}

}  // namespace opossum
