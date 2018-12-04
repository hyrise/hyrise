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
    manager.add_table("SomeTable", load_table("src/test/tables/int_int_int_string_float_calibration.tbl", 1u));
    manager.add_table("IntString", load_table("src/test/tables/int_string.tbl", 1u));
  }

  const std::vector<CalibrationColumnSpecification> _columns{
      {"column_pk", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"a", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"b", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"c", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"d", DataType::String, "uniform", false, 2, EncodingType::Unencoded},
      {"e", DataType::Float, "uniform", false, 2, EncodingType::Unencoded},
  };
};

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValueInt) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",   EncodingType::Unencoded,
                                                                DataType::Int, EncodingType::Unencoded,
                                                                DataType::Int, EncodingType::Unencoded,
                                                                DataType::Int, 0.1f,
                                                                false,         6};

  const auto table = StoredTableNode::make("SomeTable");
  const auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a <= 1000000", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValueString) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",
                                                                EncodingType::Unencoded,
                                                                DataType::String,
                                                                EncodingType::Unencoded,
                                                                DataType::Int,
                                                                EncodingType::Unencoded,
                                                                DataType::Int,
                                                                0.1f,
                                                                false,
                                                                6};

  const auto table = StoredTableNode::make("SomeTable");
  const auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("d <= 'C'", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValueFloat) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",     EncodingType::Unencoded,
                                                                DataType::Float, EncodingType::Unencoded,
                                                                DataType::Int,   EncodingType::Unencoded,
                                                                DataType::Int,   0.1f,
                                                                false,           6};

  const auto table = StoredTableNode::make("SomeTable");
  const auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("e <= 0.1f", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnColumn) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",   EncodingType::Unencoded,
                                                                DataType::Int, EncodingType::Unencoded,
                                                                DataType::Int, EncodingType::Unencoded,
                                                                DataType::Int, 0.1f,
                                                                false,         6};
  const auto table = StoredTableNode::make("SomeTable");
  const auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_column({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a <= b", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, Like) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",
                                                                EncodingType::Unencoded,
                                                                DataType::String,
                                                                EncodingType::Unencoded,
                                                                DataType::Int,
                                                                EncodingType::Unencoded,
                                                                DataType::Int,
                                                                0.1f,
                                                                false,
                                                                6};

  const auto table = StoredTableNode::make("SomeTable");
  const auto predicate = CalibrationQueryGeneratorPredicate::generate_predicate_like({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("d LIKE 'C%'", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, EquiOnStrings) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",
                                                                EncodingType::Unencoded,
                                                                DataType::String,
                                                                EncodingType::Unencoded,
                                                                DataType::Int,
                                                                EncodingType::Unencoded,
                                                                DataType::Int,
                                                                0.1f,
                                                                false,
                                                                6};

  const std::vector<CalibrationColumnSpecification> columns{
      {"a", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"b", DataType::String, "uniform", false, 2, EncodingType::Unencoded},
  };

  const auto table = StoredTableNode::make("IntString");
  const auto predicates =
      CalibrationQueryGeneratorPredicate::generate_predicate_equi_on_strings({table, columns, configuration});

  ASSERT_TRUE(predicates);
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, BetweenValueValue) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",   EncodingType::Unencoded,
                                                                DataType::Int, EncodingType::Unencoded,
                                                                DataType::Int, EncodingType::Unencoded,
                                                                DataType::Int, 0.1f,
                                                                false,         6};

  const auto table = StoredTableNode::make("SomeTable");
  const auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_between_value_value({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a BETWEEN 0 AND 1000000", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, BetweenColumnColumn) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",   EncodingType::Unencoded,
                                                                DataType::Int, EncodingType::Unencoded,
                                                                DataType::Int, EncodingType::Unencoded,
                                                                DataType::Int, 0.1f,
                                                                false,         6};
  const auto table = StoredTableNode::make("SomeTable");

  const auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_between_column_column({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a BETWEEN b AND c", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, Or) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",   EncodingType::Unencoded,
                                                                    DataType::Int, EncodingType::Unencoded,
                                                                    DataType::Int, EncodingType::Unencoded,
                                                                    DataType::Int, 0.1f,
                                                                    false,         6};
  const auto table = StoredTableNode::make("SomeTable");

  const auto predicate =
          CalibrationQueryGeneratorPredicate::generate_predicate_or({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a <= 1000000 OR a <= 5000000", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValue) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",   EncodingType::Unencoded,
                                                                DataType::Int, EncodingType::Unencoded,
                                                                DataType::Int, EncodingType::Unencoded,
                                                                DataType::Int, 0.1f,
                                                                false,         6};

  const auto table = StoredTableNode::make("SomeTable");

  const auto predicates = CalibrationQueryGeneratorPredicate::generate_predicates(
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value, _columns, table, configuration);

  ASSERT_FALSE(predicates.empty());
  ASSERT_EQ(predicates.size(), 2);

  std::vector<std::string> predicate_column_names;
  predicate_column_names.reserve(predicates.size());
  for (const auto& p : predicates) {
    predicate_column_names.push_back(p->predicate()->as_column_name());
  }

  // Index and TableScan
  ASSERT_THAT(predicate_column_names, testing::ElementsAre("a <= 1000000", "a <= 1000000"));
}

}  // namespace opossum
