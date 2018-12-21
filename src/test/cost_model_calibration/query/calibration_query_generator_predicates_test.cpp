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
    manager.add_table("SomeTable", load_table("src/test/tables/int_int_int_string_float_calibration.tbl", 1u));
    manager.add_table("IntString", load_table("src/test/tables/int_string.tbl", 1u));
    manager.add_table("CostModelCalibration",
                      load_table("src/test/tables/cost_model_calibration/cost_model_calibration.tbl", 1u));
  }

  const std::vector<CalibrationColumnSpecification> _columns{
      {"column_pk", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"a", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"b", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"c", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"d", DataType::String, "uniform", false, 2, EncodingType::Unencoded},
      {"e", DataType::Float, "uniform", false, 2, EncodingType::Unencoded},
  };

  const std::vector<CalibrationColumnSpecification> _cost_model_calibration_columns{
      {"column_pk", DataType::Int, "uniform", false, 2, EncodingType::Dictionary},
      {"column_a", DataType::Int, "uniform", false, 2, EncodingType::Dictionary},
      {"column_b", DataType::Long, "uniform", false, 2, EncodingType::Dictionary},
      {"column_c", DataType::Float, "uniform", false, 2, EncodingType::Dictionary},
      {"column_d", DataType::Double, "uniform", false, 2, EncodingType::Dictionary},
      {"column_e", DataType::String, "uniform", false, 2, EncodingType::Dictionary},
      {"column_g", DataType::Int, "uniform", false, 2, EncodingType::Dictionary},
      {"column_h", DataType::Long, "uniform", false, 2, EncodingType::Dictionary},
      {"column_i", DataType::Float, "uniform", false, 2, EncodingType::Dictionary},
      {"column_j", DataType::Double, "uniform", false, 2, EncodingType::Dictionary},
      {"column_k", DataType::String, "uniform", false, 2, EncodingType::Dictionary},
      {"column_l", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"column_m", DataType::Long, "uniform", false, 2, EncodingType::Unencoded},
      {"column_n", DataType::Float, "uniform", false, 2, EncodingType::Unencoded},
      {"column_o", DataType::Double, "uniform", false, 2, EncodingType::Unencoded},
      {"column_p", DataType::String, "uniform", false, 2, EncodingType::Unencoded},
      {"column_q", DataType::Int, "uniform", false, 2, EncodingType::Unencoded},
      {"column_r", DataType::Long, "uniform", false, 2, EncodingType::Unencoded},
      {"column_s", DataType::Float, "uniform", false, 2, EncodingType::Unencoded},
      {"column_t", DataType::Double, "uniform", false, 2, EncodingType::Unencoded},
      {"column_u", DataType::String, "uniform", false, 2, EncodingType::Unencoded},
      {"column_v", DataType::Int, "uniform", false, 2, EncodingType::FrameOfReference},
      {"column_w", DataType::Long, "uniform", false, 2, EncodingType::FrameOfReference},
      {"column_x", DataType::Int, "uniform", false, 2, EncodingType::FrameOfReference},
      {"column_y", DataType::Long, "uniform", false, 2, EncodingType::FrameOfReference},
      {"column_z", DataType::Int, "uniform", false, 2, EncodingType::RunLength},
      {"column_aa", DataType::Long, "uniform", false, 2, EncodingType::RunLength},
      {"column_ab", DataType::Float, "uniform", false, 2, EncodingType::RunLength},
      {"column_ac", DataType::Double, "uniform", false, 2, EncodingType::RunLength},
      {"column_ad", DataType::String, "uniform", false, 2, EncodingType::RunLength},
      {"column_ae", DataType::Int, "uniform", false, 2, EncodingType::RunLength},
      {"column_af", DataType::Long, "uniform", false, 2, EncodingType::RunLength},
      {"column_ag", DataType::Float, "uniform", false, 2, EncodingType::RunLength},
      {"column_ah", DataType::Double, "uniform", false, 2, EncodingType::RunLength},
      {"column_ai", DataType::String, "uniform", false, 2, EncodingType::RunLength},
      {"column_aj", DataType::String, "uniform", false, 2, EncodingType::FixedStringDictionary},
      {"column_ak", DataType::String, "uniform", false, 2, EncodingType::FixedStringDictionary},
  };
};

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValueInt) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",
                                                                DataType::Int,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                0.1f,
                                                                false,
                                                                6};

  const auto table = StoredTableNode::make("SomeTable");
  const auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a <= 1000000", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValueString) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",
                                                                DataType::String,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
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
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",
                                                                DataType::Float,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                0.1f,
                                                                false,
                                                                6};

  const auto table = StoredTableNode::make("SomeTable");
  const auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("e <= 0.1f", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnColumn) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",
                                                                DataType::Int,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                0.1f,
                                                                false,
                                                                6};
  const auto table = StoredTableNode::make("SomeTable");
  const auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_column_column({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a <= b", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, Like) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",
                                                                DataType::String,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
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
                                                                DataType::String,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
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
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",
                                                                DataType::Int,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                0.1f,
                                                                false,
                                                                6};

  const auto table = StoredTableNode::make("SomeTable");
  const auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_between_value_value({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a BETWEEN 0 AND 1000000", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, BetweenColumnColumn) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",
                                                                DataType::Int,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                0.1f,
                                                                false,
                                                                6};
  const auto table = StoredTableNode::make("SomeTable");

  const auto predicate =
      CalibrationQueryGeneratorPredicate::generate_predicate_between_column_column({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a BETWEEN b AND c", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, Or) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",
                                                                DataType::Int,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                0.1f,
                                                                false,
                                                                6};
  const auto table = StoredTableNode::make("SomeTable");

  const auto predicate = CalibrationQueryGeneratorPredicate::generate_predicate_or({table, _columns, configuration});

  ASSERT_TRUE(predicate);
  EXPECT_EQ("a <= 1000000 OR a <= 5000000", predicate->as_column_name());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, ColumnValue) {
  CalibrationQueryGeneratorPredicateConfiguration configuration{"SomeTable",
                                                                DataType::Int,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                EncodingType::Unencoded,
                                                                0.1f,
                                                                false,
                                                                6};

  const auto table = StoredTableNode::make("SomeTable");

  const auto predicates = CalibrationQueryGeneratorPredicate::generate_predicates(
      CalibrationQueryGeneratorPredicate::generate_predicate_column_value, _columns, table, configuration, true);

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

TEST_F(CalibrationQueryGeneratorPredicatesTest, GeneratePermutationsColumnValue) {
  const std::vector<std::pair<std::string, size_t>> tables{{"CostModelCalibration", 2}};
  CalibrationConfiguration configuration{};
  configuration.encodings = {EncodingType::Unencoded, EncodingType::Dictionary};
  configuration.data_types = {DataType::Int, DataType::String};
  configuration.selectivities = {0.1f, 0.5f};

  const auto permutations = CalibrationQueryGeneratorPredicate::generate_predicate_permutations(tables, configuration);

  // 1 table * 2^3 encodings * 2 data types * 2 selectivities * 2 reference/non-reference
  ASSERT_EQ(64, permutations.size());

  std::vector<std::shared_ptr<AbstractLQPNode>> predicates;
  const auto table = StoredTableNode::make("CostModelCalibration");
  for (const auto& permutation : permutations) {
    const auto generated_predicates = CalibrationQueryGeneratorPredicate::generate_predicates(
        CalibrationQueryGeneratorPredicate::generate_predicate_column_value, _cost_model_calibration_columns, table,
        permutation);

    for (const auto& generated_predicate : generated_predicates) {
      predicates.push_back(generated_predicate);
    }
  }

  ASSERT_EQ(permutations.size(), predicates.size());
}

TEST_F(CalibrationQueryGeneratorPredicatesTest, GeneratePermutationsColumnColumn) {
  const std::vector<std::pair<std::string, size_t>> tables{{"CostModelCalibration", 2}};
  CalibrationConfiguration configuration{};
  configuration.encodings = {EncodingType::Unencoded, EncodingType::Dictionary};
  configuration.data_types = {DataType::Int, DataType::String};
  configuration.selectivities = {0.1f, 0.5f};

  const auto permutations = CalibrationQueryGeneratorPredicate::generate_predicate_permutations(tables, configuration);

  // 1 table * 2^3 encodings * 2 data types * 2 selectivities * 2 reference/non-reference
  ASSERT_EQ(64, permutations.size());

  std::vector<std::shared_ptr<AbstractLQPNode>> predicates;
  const auto table = StoredTableNode::make("CostModelCalibration");
  for (const auto& permutation : permutations) {
    const auto generated_predicates = CalibrationQueryGeneratorPredicate::generate_predicates(
        CalibrationQueryGeneratorPredicate::generate_predicate_column_column, _cost_model_calibration_columns, table,
        permutation);

    for (const auto& generated_predicate : generated_predicates) {
      predicates.push_back(generated_predicate);
    }
  }

  ASSERT_EQ(permutations.size(), predicates.size());
}

}  // namespace opossum
