#include <gmock/gmock.h>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
//#include "gtest/gtest.h"

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
    manager.add_table("IntString", load_table("src/test/tables/int_string.tbl", 1u));
  }
};

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValueInt) {
  const CalibrationColumnSpecification filter_column{"a", DataType::Int, "uniform", false, 2, EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{
      "SomeTable", EncodingType::Unencoded, DataType::Int, 0.1f, false, 6};

  const auto table = StoredTableNode::make("SomeTable");
  auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value(table, filter_column, configuration);

  ASSERT_FALSE(predicate.empty());
  ASSERT_EQ(predicate.size(), 1);
  EXPECT_EQ("a <= 1000000", predicate.front()->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValueString) {
  const CalibrationColumnSpecification filter_column{"a", DataType::String,       "uniform", false,
                                                     2,   EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{
      "SomeTable", EncodingType::Unencoded, DataType::String, 0.1f, false, 6};

  const auto table = StoredTableNode::make("SomeTable");
  auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value(table, filter_column, configuration);

  ASSERT_FALSE(predicate.empty());
  ASSERT_EQ(predicate.size(), 1);
  EXPECT_EQ("a <= 'C'", predicate.front()->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValueFloat) {
  const CalibrationColumnSpecification filter_column{"a", DataType::Float,        "uniform", false,
                                                     2,   EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{
      "SomeTable", EncodingType::Unencoded, DataType::Float, 0.1f, false, 6};

  const auto table = StoredTableNode::make("SomeTable");
  auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value(table, filter_column, configuration);

  ASSERT_FALSE(predicate.empty());
  ASSERT_EQ(predicate.size(), 1);
  EXPECT_EQ("a <= 0.1f", predicate.front()->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnColumn) {
  const CalibrationColumnSpecification filter_column{"a", DataType::Int, "uniform", false, 1, EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{
      "SomeTable", EncodingType::Unencoded, DataType::Int, 0.1f, false, 6};

  const auto table = StoredTableNode::make("SomeTable");
  auto predicates =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_column(table, filter_column, configuration);

  ASSERT_FALSE(predicates.empty());
  ASSERT_EQ(predicates.size(), 2);
  std::vector<std::string> predicate_column_names{};
  for (const auto& p : predicates) {
    predicate_column_names.push_back(p->as_column_name());
  }
  ASSERT_THAT(predicate_column_names, testing::ElementsAre("a <= b", "a <= c"));
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, Like) {
  const CalibrationColumnSpecification filter_column{"a", DataType::String,       "uniform", false,
                                                     2,   EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{
      "SomeTable", EncodingType::Unencoded, DataType::String, 0.1f, false, 6};

  //  const std::vector<std::pair<DataType, std::string>> columns{{DataType::String, "a"}};
  const auto table = StoredTableNode::make("SomeTable");
  auto predicate = CalibrationQueryGeneratorPredicate::generate_predicate_like(table, filter_column, configuration);

  ASSERT_FALSE(predicate.empty());
  ASSERT_EQ(predicate.size(), 1);
  EXPECT_EQ("a LIKE 'C%'", predicate.front()->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, EquiOnStrings) {
  const CalibrationColumnSpecification filter_column{"b", DataType::String,       "uniform", false,
                                                     2,   EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{
      "IntString", EncodingType::Unencoded, DataType::String, 0.1f, false, 14};

  const auto table = StoredTableNode::make("IntString");
  auto predicates =
      CalibrationQueryGeneratorPredicate::generate_predicate_equi_on_strings(table, filter_column, configuration);

  ASSERT_FALSE(predicates.empty());
  ASSERT_EQ(predicates.size(), 1);
  //  EXPECT_EQ("a = Parameter[id=0]", predicates.front()->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, BetweenValueValue) {
  const CalibrationColumnSpecification filter_column{"a", DataType::Int, "uniform", false, 10, EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{
      "SomeTable", EncodingType::Unencoded, DataType::Int, 0.1f, false, 6};

  //  const std::vector<std::pair<DataType, std::string>> columns{{DataType::Int, "a"}};
  const auto table = StoredTableNode::make("SomeTable");
  auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_between_value_value(table, filter_column, configuration);

  ASSERT_FALSE(predicate.empty());
  ASSERT_EQ(predicate.size(), 1);
  EXPECT_EQ("a BETWEEN 0 AND 1000000", predicate.front()->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, BetweenColumnColumn) {
  const CalibrationColumnSpecification filter_column{"a", DataType::Int, "uniform", false, 2, EncodingType::Unencoded};

  CalibrationQueryGeneratorPredicateConfiguration configuration{
      "SomeTable", EncodingType::Unencoded, DataType::Int, 0.1f, false, 6};

  const auto table = StoredTableNode::make("SomeTable");
  //predicate_expression
  auto predicates =
      CalibrationQueryGeneratorPredicate::generate_predicate_between_column_column(table, filter_column, configuration);

  ASSERT_FALSE(predicates.empty());
  ASSERT_EQ(predicates.size(), 2);

  std::vector<std::string> predicate_column_names;
  predicate_column_names.reserve(predicates.size());
  for (const auto& p : predicates) {
    predicate_column_names.push_back(p->as_column_name());
  }
  ASSERT_THAT(predicate_column_names, testing::ElementsAre("a BETWEEN b AND c", "a BETWEEN c AND b"));
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValue) {
  std::vector<CalibrationColumnSpecification> column_specifications{
      {"a", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"b", DataType::Int, "uniform", false, 2, EncodingType::Dictionary},
      {"c", DataType::String, "uniform", false, 2, EncodingType::Unencoded},
      {"d", DataType::String, "uniform", false, 2, EncodingType::Dictionary}};
  CalibrationQueryGeneratorPredicateConfiguration configuration{
      "SomeTable", EncodingType::Dictionary, DataType::Int, 0.1f, false, 6};

  const auto table = StoredTableNode::make("SomeTable");

  auto predicates = CalibrationQueryGeneratorPredicate::generate_predicates(
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value, column_specifications, table, configuration);

  ASSERT_FALSE(predicates.empty());
  ASSERT_EQ(predicates.size(), 2);

  std::vector<std::string> predicate_column_names;
  predicate_column_names.reserve(predicates.size());
  for (const auto& p : predicates) {
    predicate_column_names.push_back(p->predicate()->as_column_name());
  }

  // Index and TableScan
  ASSERT_THAT(predicate_column_names, testing::ElementsAre("b <= 1000000", "b <= 1000000"));
}

}  // namespace opossum
