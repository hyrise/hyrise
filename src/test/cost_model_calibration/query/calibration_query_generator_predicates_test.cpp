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
    auto& manager = Hyrise::get().storage_manager;
    manager.add_table("SomeTable", load_table("src/test/tables/int_int_int_string_float_calibration.tbl", 1u));
    manager.add_table("IntString", load_table("resources/test_data/tbl/int_string.tbl", 1u));
    manager.add_table("CostModelCalibration",
                      load_table("src/test/tables/cost_model_calibration/cost_model_calibration.tbl", 1u));
  }

  const std::vector<CalibrationColumnSpecification> _columns{
      {"column_pk", ColumnID{0}, DataType::Int, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"a", ColumnID{1}, DataType::Int, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"b", ColumnID{2}, DataType::Int, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"c", ColumnID{3}, DataType::Int, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"d", ColumnID{4}, DataType::String, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"e", ColumnID{5}, DataType::Float, "uniform", false, 2, EncodingType::Unencoded, 1},
  };

  const std::vector<CalibrationColumnSpecification> _cost_model_calibration_columns{
      {"column_pk", ColumnID{0}, DataType::Int, "uniform", false, 2, EncodingType::Dictionary, 1},
      {"column_a", ColumnID{1}, DataType::Int, "uniform", false, 2, EncodingType::Dictionary, 1},
      {"column_b", ColumnID{2}, DataType::Long, "uniform", false, 2, EncodingType::Dictionary, 1},
      {"column_c", ColumnID{3}, DataType::Float, "uniform", false, 2, EncodingType::Dictionary, 1},
      {"column_d", ColumnID{4}, DataType::Double, "uniform", false, 2, EncodingType::Dictionary, 1},
      {"column_e", ColumnID{5}, DataType::String, "uniform", false, 2, EncodingType::Dictionary, 1},
      {"column_g", ColumnID{6}, DataType::Int, "uniform", false, 2, EncodingType::Dictionary, 1},
      {"column_h", ColumnID{7}, DataType::Long, "uniform", false, 2, EncodingType::Dictionary, 1},
      {"column_i", ColumnID{8}, DataType::Float, "uniform", false, 2, EncodingType::Dictionary, 1},
      {"column_j", ColumnID{9}, DataType::Double, "uniform", false, 2, EncodingType::Dictionary, 1},
      {"column_k", ColumnID{10}, DataType::String, "uniform", false, 2, EncodingType::Dictionary, 1},
      {"column_l", ColumnID{11}, DataType::Int, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"column_m", ColumnID{12}, DataType::Long, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"column_n", ColumnID{13}, DataType::Float, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"column_o", ColumnID{14}, DataType::Double, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"column_p", ColumnID{15}, DataType::String, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"column_q", ColumnID{16}, DataType::Int, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"column_r", ColumnID{17}, DataType::Long, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"column_s", ColumnID{18}, DataType::Float, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"column_t", ColumnID{19}, DataType::Double, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"column_u", ColumnID{20}, DataType::String, "uniform", false, 2, EncodingType::Unencoded, 1},
      {"column_v", ColumnID{21}, DataType::Int, "uniform", false, 2, EncodingType::FrameOfReference, 1},
      {"column_w", ColumnID{22}, DataType::Long, "uniform", false, 2, EncodingType::FrameOfReference, 1},
      {"column_x", ColumnID{23}, DataType::Int, "uniform", false, 2, EncodingType::FrameOfReference, 1},
      {"column_y", ColumnID{24}, DataType::Long, "uniform", false, 2, EncodingType::FrameOfReference, 1},
      {"column_z", ColumnID{25}, DataType::Int, "uniform", false, 2, EncodingType::RunLength, 1},
      {"column_aa", ColumnID{26}, DataType::Long, "uniform", false, 2, EncodingType::RunLength, 1},
      {"column_ab", ColumnID{27}, DataType::Float, "uniform", false, 2, EncodingType::RunLength, 1},
      {"column_ac", ColumnID{28}, DataType::Double, "uniform", false, 2, EncodingType::RunLength, 1},
      {"column_ad", ColumnID{29}, DataType::String, "uniform", false, 2, EncodingType::RunLength, 1},
      {"column_ae", ColumnID{30}, DataType::Int, "uniform", false, 2, EncodingType::RunLength, 1},
      {"column_af", ColumnID{31}, DataType::Long, "uniform", false, 2, EncodingType::RunLength, 1},
      {"column_ag", ColumnID{32}, DataType::Float, "uniform", false, 2, EncodingType::RunLength, 1},
      {"column_ah", ColumnID{33}, DataType::Double, "uniform", false, 2, EncodingType::RunLength, 1},
      {"column_ai", ColumnID{34}, DataType::String, "uniform", false, 2, EncodingType::RunLength, 1},
      {"column_aj", ColumnID{35}, DataType::String, "uniform", false, 2, EncodingType::FixedStringDictionary, 1},
      {"column_ak", ColumnID{36}, DataType::String, "uniform", false, 2, EncodingType::FixedStringDictionary, 1},
  };
};
/*
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
*/
TEST_F(CalibrationQueryGeneratorPredicatesTest, GeneratePermutationsColumnValue) {
  const std::vector<std::pair<std::string, size_t>> tables{{"CostModelCalibration", 2}};
  CalibrationConfiguration configuration{};
  configuration.encodings = {EncodingType::Unencoded, EncodingType::Dictionary};
  configuration.data_types = {DataType::Int, DataType::String};
  configuration.selectivities = {0.1f, 0.5f};

  const auto permutations = CalibrationQueryGeneratorPredicate::generate_predicate_permutations(tables, configuration);
  const std::set<CalibrationQueryGeneratorPredicateConfiguration> permutation_set(permutations.begin(),
                                                                                  permutations.end());

  ASSERT_EQ(permutation_set.size(), permutations.size());

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
