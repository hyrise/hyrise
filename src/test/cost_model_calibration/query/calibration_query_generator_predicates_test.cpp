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
    manager.add_table("SomeTable", load_table("src/test/tables/int_int_int_calibration.tbl", 1u));
  }
};

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValueInt) {
  const CalibrationColumnSpecification filter_column{"a", DataType::Int, "uniform", false, 2, EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable", EncodingType::Unencoded, DataType::Int, 0.1f, false};

//  const std::vector<std::pair<DataType, std::string>> columns{{DataType::Int, "a"}};
  const auto table = StoredTableNode::make("SomeTable");
  auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value(table, filter_column, configuration);

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a <= 0", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValueString) {
  const CalibrationColumnSpecification filter_column{"a", DataType::String,       "uniform", false,
                                                     2,   EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable", EncodingType::Unencoded, DataType::String, 0.1f, false};

//  const std::vector<std::pair<DataType, std::string>> columns{{DataType::String, "a"}};
  const auto table = StoredTableNode::make("SomeTable");
  auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value(table, filter_column, configuration);

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a <= 'C'", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValueFloat) {
  const CalibrationColumnSpecification filter_column{"a", DataType::Float,        "uniform", false,
                                                     2,   EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable", EncodingType::Unencoded, DataType::Float, 0.1f, false};

//  const std::vector<std::pair<DataType, std::string>> columns{{DataType::Float, "a"}};
  const auto table = StoredTableNode::make("SomeTable");
  auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value(table, filter_column, configuration);

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a <= 0.1f", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnColumn) {
  const CalibrationColumnSpecification filter_column{"a", DataType::Int, "uniform", false, 1, EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable", EncodingType::Unencoded, DataType::Int, 0.1f, false};

  const auto table = StoredTableNode::make("SomeTable");
  auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_column(table, filter_column, configuration);

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a <= b", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, Like) {
  const CalibrationColumnSpecification filter_column{"a", DataType::String,       "uniform", false,
                                                     2,   EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable", EncodingType::Unencoded, DataType::String, 0.1f, false};

//  const std::vector<std::pair<DataType, std::string>> columns{{DataType::String, "a"}};
  const auto table = StoredTableNode::make("SomeTable");
  auto predicate = CalibrationQueryGeneratorPredicate::generate_predicate_like(table, filter_column, configuration);

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a LIKE 'C%'", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, EquiOnStrings) {
  const CalibrationColumnSpecification filter_column{"a", DataType::String,       "uniform", false,
                                                     2,   EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable", EncodingType::Unencoded, DataType::Int, 0.1f, false};

  const auto table = StoredTableNode::make("SomeTable");
  auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_equi_on_strings(table, filter_column, configuration);

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a = Parameter[id=0]", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, BetweenValueValue) {
  const CalibrationColumnSpecification filter_column{"a", DataType::Int, "uniform", false, 10, EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable", EncodingType::Unencoded, DataType::Int, 0.1f, false};

//  const std::vector<std::pair<DataType, std::string>> columns{{DataType::Int, "a"}};
  const auto table = StoredTableNode::make("SomeTable");
  auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_between_value_value(table, filter_column, configuration);

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a BETWEEN 1 AND 5", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, BetweenColumnColumn) {
  const CalibrationColumnSpecification filter_column{"a", DataType::Int, "uniform", false, 2, EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable", EncodingType::Unencoded, DataType::Int, 0.1f, false};

  const auto table = StoredTableNode::make("SomeTable");
  auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_between_column_column(table, filter_column, configuration);

  ASSERT_TRUE(predicate);
  std::cout << predicate->as_column_name() << std::endl;
  EXPECT_TRUE("a BETWEEN b AND c" == predicate->as_column_name() || "a BETWEEN c AND b" == predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValue) {
  std::vector<CalibrationColumnSpecification> column_specifications{
      {"a", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"b", DataType::Int, "uniform", false, 2, EncodingType::Dictionary},
      {"c", DataType::String, "uniform", false, 2, EncodingType::Unencoded},
      {"d", DataType::String, "uniform", false, 2, EncodingType::Dictionary}};
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable", EncodingType::Dictionary, DataType::Int, 0.1f, false};

  const auto table = StoredTableNode::make("SomeTable");

  auto predicate = CalibrationQueryGeneratorPredicate::generate_predicates(
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value, column_specifications, table, configuration);

  ASSERT_TRUE(predicate);
  EXPECT_EQ("b <= 0", predicate->predicate->as_column_name());
}

}  // namespace opossum
