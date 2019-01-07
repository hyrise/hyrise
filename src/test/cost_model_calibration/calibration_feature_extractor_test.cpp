#include "base_test.hpp"
#include "gtest/gtest.h"

#include "calibration_feature_extractor.hpp"
#include "expression/expression_functional.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/join_mpsm.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"

#include "operators/table_wrapper.hpp"

namespace opossum {

class CalibrationFeatureExtractorTest : public BaseTest {
 protected:
  void SetUp() override {
    const auto int_int = load_table("src/test/tables/int_int_shuffled.tbl", 7);

    a = PQPColumnExpression::from_table(*int_int, "a");
    b = PQPColumnExpression::from_table(*int_int, "b");

    _int_int = std::make_shared<TableWrapper>(int_int);
    _int_int->execute();
  }

 protected:
  std::shared_ptr<TableWrapper> _int_int;

  std::shared_ptr<PQPColumnExpression> a, b;
};

template <typename T>
class CalibrationFeatureExtractorJoinTest : public BaseTest {
 protected:
  void SetUp() override {
    const auto int_int = load_table("src/test/tables/int_int.tbl", 7);
    const auto int_string = load_table("src/test/tables/int_string.tbl", 7);

    _int_int = std::make_shared<TableWrapper>(int_int);
    _int_int->execute();

    _int_string = std::make_shared<TableWrapper>(int_string);
    _int_string->execute();
  }

 protected:
  std::shared_ptr<TableWrapper> _int_int, _int_string;
};

TEST_F(CalibrationFeatureExtractorTest, ExtractSimpleComparison) {
  // set up some TableScanOperator

  auto predicate = equals_(a, 6);

  const auto table_scan = std::make_shared<TableScan>(_int_int, predicate);
  table_scan->execute();

  const auto calibration_example = CalibrationFeatureExtractor::extract_features(table_scan);

  EXPECT_TRUE(calibration_example.table_scan_features);
  EXPECT_EQ("Unencoded", calibration_example.table_scan_features->first_column.column_encoding);
  EXPECT_EQ("undefined", calibration_example.table_scan_features->second_column.column_encoding);
  EXPECT_EQ("undefined", calibration_example.table_scan_features->third_column.column_encoding);
  EXPECT_EQ(calibration_example.table_scan_features->number_of_computable_or_column_expressions, 2);
}

TEST_F(CalibrationFeatureExtractorTest, ExtractBetween) {
  // set up some TableScanOperator

  auto predicate = between_(a, 6, 10);

  const auto table_scan = std::make_shared<TableScan>(_int_int, predicate);
  table_scan->execute();

  const auto calibration_example = CalibrationFeatureExtractor::extract_features(table_scan);

  EXPECT_TRUE(calibration_example.table_scan_features);
  EXPECT_EQ(calibration_example.table_scan_features->first_column.column_encoding, "Unencoded");
  EXPECT_EQ(calibration_example.table_scan_features->second_column.column_encoding, "undefined");
  EXPECT_EQ(calibration_example.table_scan_features->number_of_computable_or_column_expressions, 2);
}

TEST_F(CalibrationFeatureExtractorTest, ExtractOr) {
  // set up some TableScanOperator

  auto predicate = or_(equals_(a, 6), equals_(b, 10));

  const auto table_scan = std::make_shared<TableScan>(_int_int, predicate);
  table_scan->execute();

  const auto calibration_example = CalibrationFeatureExtractor::extract_features(table_scan);

  EXPECT_TRUE(calibration_example.table_scan_features);
  EXPECT_EQ(calibration_example.table_scan_features->first_column.column_encoding, "undefined");
  EXPECT_EQ(calibration_example.table_scan_features->second_column.column_encoding, "undefined");
  EXPECT_EQ(calibration_example.table_scan_features->third_column.column_encoding, "undefined");
  EXPECT_EQ(calibration_example.table_scan_features->number_of_computable_or_column_expressions, 5);
}

using JoinTypes = ::testing::Types<JoinHash, JoinIndex, JoinSortMerge, JoinNestedLoop, JoinMPSM>;
TYPED_TEST_CASE(CalibrationFeatureExtractorJoinTest, JoinTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(CalibrationFeatureExtractorJoinTest, ExtractJoin) {
  const auto join = std::make_shared<TypeParam>(this->_int_int, this->_int_string, JoinMode::Inner,
                                                ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
  join->execute();

  const auto calibration_example = CalibrationFeatureExtractor::extract_features(join);

  EXPECT_TRUE(calibration_example.join_features);
}

}  // namespace opossum
