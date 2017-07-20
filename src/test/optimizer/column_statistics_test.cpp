#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "optimizer/column_statistics.hpp"

namespace opossum {

class ColumnStatisticsTest : public BaseTest {
 protected:
  void SetUp() override {
    _table = load_table("src/test/tables/int_float_double_string.tbl", 0);
    _column_statistics_int = std::make_shared<ColumnStatistics<int>>(ColumnID(0), _table);
    _column_statistics_float = std::make_shared<ColumnStatistics<float>>(ColumnID(1), _table);
    _column_statistics_double = std::make_shared<ColumnStatistics<double>>(ColumnID(2), _table);
    _column_statistics_string = std::make_shared<ColumnStatistics<std::string>>(ColumnID(3), _table);
  }

  template <typename T>
  void predict_selectivities_and_compare(const std::shared_ptr<ColumnStatistics<T>> column_statistic,
                                         const ScanType scan_type, const std::vector<T> &values,
                                         const std::vector<float> &expected_selectivities) {
    auto expected_selectivities_itr = expected_selectivities.begin();
    for (const auto value : values) {
      auto result_container = column_statistic->predicate_selectivity(scan_type, AllTypeVariant(value));
      EXPECT_FLOAT_EQ(result_container.selectivity, *expected_selectivities_itr++);
    }
  }

  template <typename T>
  void predict_selectivities_and_compare(const std::shared_ptr<ColumnStatistics<T>> column_statistic,
                                         const ScanType scan_type, const std::vector<std::pair<T, T>> &values,
                                         const std::vector<float> &expected_selectivities) {
    auto expected_selectivities_itr = expected_selectivities.begin();
    for (const auto value_pair : values) {
      auto result_container = column_statistic->predicate_selectivity(scan_type, AllTypeVariant(value_pair.first),
                                                                      AllTypeVariant(value_pair.second));
      EXPECT_FLOAT_EQ(result_container.selectivity, *expected_selectivities_itr++);
    }
  }

  template <typename T>
  void predict_selectivities_for_stored_procedures_and_compare(
      const std::shared_ptr<ColumnStatistics<T>> column_statistic, const ScanType scan_type,
      const std::vector<T> &values2, const std::vector<float> &expected_selectivities) {
    auto expected_selectivities_itr = expected_selectivities.begin();
    for (const auto value2 : values2) {
      auto result_container =
          column_statistic->predicate_selectivity(scan_type, ValuePlaceholder(0), AllTypeVariant(value2));
      EXPECT_FLOAT_EQ(result_container.selectivity, *expected_selectivities_itr++);
    }
  }

  std::shared_ptr<Table> _table;
  std::shared_ptr<ColumnStatistics<int>> _column_statistics_int;
  std::shared_ptr<ColumnStatistics<float>> _column_statistics_float;
  std::shared_ptr<ColumnStatistics<double>> _column_statistics_double;
  std::shared_ptr<ColumnStatistics<std::string>> _column_statistics_string;

  //  {below min, min, middle, max, above max}
  std::vector<int> _int_values{0, 1, 3, 6, 7};
  std::vector<float> _float_values{0.f, 1.f, 3.f, 6.f, 7.f};
  std::vector<double> _double_values{0., 1., 3., 6., 7.};
  std::vector<std::string> _string_values{"a", "b", "c", "g", "h"};
};

TEST_F(ColumnStatisticsTest, NotEqualTest) {
  ScanType scan_type = ScanType::OpNotEquals;

  std::vector<float> selectivities{1.f, 5.f / 6.f, 5.f / 6.f, 5.f / 6.f, 1.f};
  predict_selectivities_and_compare(_column_statistics_int, scan_type, _int_values, selectivities);
  predict_selectivities_and_compare(_column_statistics_float, scan_type, _float_values, selectivities);
  predict_selectivities_and_compare(_column_statistics_double, scan_type, _double_values, selectivities);
  predict_selectivities_and_compare(_column_statistics_string, scan_type, _string_values, selectivities);
}

TEST_F(ColumnStatisticsTest, EqualsTest) {
  ScanType scan_type = ScanType::OpEquals;

  std::vector<float> selectivities{0.f, 1.f / 6.f, 1.f / 6.f, 1.f / 6.f, 0.f};
  predict_selectivities_and_compare(_column_statistics_int, scan_type, _int_values, selectivities);
  predict_selectivities_and_compare(_column_statistics_float, scan_type, _float_values, selectivities);
  predict_selectivities_and_compare(_column_statistics_double, scan_type, _double_values, selectivities);
  predict_selectivities_and_compare(_column_statistics_string, scan_type, _string_values, selectivities);
}

TEST_F(ColumnStatisticsTest, LessThanTest) {
  ScanType scan_type = ScanType::OpLessThan;

  std::vector<float> selectivities_int{0.f, 0.f, 1.f / 3.f, 5.f / 6.f, 1.f};
  predict_selectivities_and_compare(_column_statistics_int, scan_type, _int_values, selectivities_int);

  std::vector<float> selectivities_float{0.f, 0.f, 0.4f, 1.f, 1.f};
  predict_selectivities_and_compare(_column_statistics_float, scan_type, _float_values, selectivities_float);
  predict_selectivities_and_compare(_column_statistics_double, scan_type, _double_values, selectivities_float);
}

TEST_F(ColumnStatisticsTest, LessEqualThanTest) {
  ScanType scan_type = ScanType::OpLessThanEquals;

  std::vector<float> selectivities_int{0.f, 1.f / 6.f, 1.f / 2.f, 1.f, 1.f};
  predict_selectivities_and_compare(_column_statistics_int, scan_type, _int_values, selectivities_int);

  std::vector<float> selectivities_float{0.f, 0.f, 0.4f, 1.f, 1.f};
  predict_selectivities_and_compare(_column_statistics_float, scan_type, _float_values, selectivities_float);
  predict_selectivities_and_compare(_column_statistics_double, scan_type, _double_values, selectivities_float);
}

TEST_F(ColumnStatisticsTest, GreaterThanTest) {
  ScanType scan_type = ScanType::OpGreaterThan;

  std::vector<float> selectivities_int{1.f, 5.f / 6.f, 1.f / 2.f, 0.f, 0.f};
  predict_selectivities_and_compare(_column_statistics_int, scan_type, _int_values, selectivities_int);

  std::vector<float> selectivities_float{1.f, 1.f, 0.6f, 0.f, 0.f};
  predict_selectivities_and_compare(_column_statistics_float, scan_type, _float_values, selectivities_float);
  predict_selectivities_and_compare(_column_statistics_double, scan_type, _double_values, selectivities_float);
}

TEST_F(ColumnStatisticsTest, GreaterEqualThanTest) {
  ScanType scan_type = ScanType::OpGreaterThanEquals;

  std::vector<float> selectivities_int{1.f, 1.f, 2.f / 3.f, 1.f / 6.f, 0.f};
  predict_selectivities_and_compare(_column_statistics_int, scan_type, _int_values, selectivities_int);

  std::vector<float> selectivities_float{1.f, 1.f, 0.6f, 0.f, 0.f};
  predict_selectivities_and_compare(_column_statistics_float, scan_type, _float_values, selectivities_float);
  predict_selectivities_and_compare(_column_statistics_double, scan_type, _double_values, selectivities_float);
}

TEST_F(ColumnStatisticsTest, BetweenTest) {
  ScanType scan_type = ScanType::OpBetween;

  std::vector<std::pair<int, int>> int_values{{-1, 0}, {-1, 2}, {1, 2}, {0, 7}, {5, 6}, {5, 8}, {7, 8}};
  std::vector<float> selectivities_int{0.f, 1.f / 3.f, 1.f / 3.f, 1.f, 1.f / 3.f, 1.f / 3.f, 0.f};
  predict_selectivities_and_compare(_column_statistics_int, scan_type, int_values, selectivities_int);

  std::vector<std::pair<float, float>> float_values{{-1.f, 0.f}, {-1.f, 2.f}, {1.f, 2.f}, {0.f, 7.f},
                                                    {5.f, 6.f},  {5.f, 8.f},  {7.f, 8.f}};
  std::vector<float> selectivities_float{0.f, 1.f / 5.f, 1.f / 5.f, 1.f, 1.f / 5.f, 1.f / 5.f, 0.f};
  predict_selectivities_and_compare(_column_statistics_float, scan_type, float_values, selectivities_float);

  std::vector<std::pair<double, double>> double_values{{-1., 0.}, {-1., 2.}, {1., 2.}, {0., 7.},
                                                       {5., 6.},  {5., 8.},  {7., 8.}};
  predict_selectivities_and_compare(_column_statistics_double, scan_type, double_values, selectivities_float);
}

TEST_F(ColumnStatisticsTest, StoredProcedureNotEqualsTest) {
  ScanType scan_type = ScanType::OpNotEquals;

  auto result_container_int = _column_statistics_int->predicate_selectivity(scan_type, ValuePlaceholder(0));
  EXPECT_FLOAT_EQ(result_container_int.selectivity, 5.f / 6.f);

  auto result_container_float = _column_statistics_float->predicate_selectivity(scan_type, ValuePlaceholder(0));
  EXPECT_FLOAT_EQ(result_container_float.selectivity, 5.f / 6.f);

  auto result_container_double = _column_statistics_double->predicate_selectivity(scan_type, ValuePlaceholder(0));
  EXPECT_FLOAT_EQ(result_container_double.selectivity, 5.f / 6.f);

  auto result_container_string = _column_statistics_string->predicate_selectivity(scan_type, ValuePlaceholder(0));
  EXPECT_FLOAT_EQ(result_container_string.selectivity, 5.f / 6.f);
}

TEST_F(ColumnStatisticsTest, StoredProcedureEqualsTest) {
  ScanType scan_type = ScanType::OpEquals;

  auto result_container_int = _column_statistics_int->predicate_selectivity(scan_type, ValuePlaceholder(0));
  EXPECT_FLOAT_EQ(result_container_int.selectivity, 1.f / 6.f);

  auto result_container_float = _column_statistics_float->predicate_selectivity(scan_type, ValuePlaceholder(0));
  EXPECT_FLOAT_EQ(result_container_float.selectivity, 1.f / 6.f);

  auto result_container_double = _column_statistics_double->predicate_selectivity(scan_type, ValuePlaceholder(0));
  EXPECT_FLOAT_EQ(result_container_double.selectivity, 1.f / 6.f);

  auto result_container_string = _column_statistics_string->predicate_selectivity(scan_type, ValuePlaceholder(0));
  EXPECT_FLOAT_EQ(result_container_string.selectivity, 1.f / 6.f);
}

TEST_F(ColumnStatisticsTest, StoredProcedureOpenEndedTest) {
  // OpLessThan, OpGreaterThan, OpLessThanEquals, OpGreaterThanEquals are same for stored procedures
  ScanType scan_type = ScanType::OpLessThan;

  auto result_container_int = _column_statistics_int->predicate_selectivity(scan_type, ValuePlaceholder(0));
  EXPECT_FLOAT_EQ(result_container_int.selectivity, 1.f / 3.f);

  auto result_container_float = _column_statistics_float->predicate_selectivity(scan_type, ValuePlaceholder(0));
  EXPECT_FLOAT_EQ(result_container_float.selectivity, 1.f / 3.f);

  auto result_container_double = _column_statistics_double->predicate_selectivity(scan_type, ValuePlaceholder(0));
  EXPECT_FLOAT_EQ(result_container_double.selectivity, 1.f / 3.f);

  auto result_container_string = _column_statistics_string->predicate_selectivity(scan_type, ValuePlaceholder(0));
  EXPECT_FLOAT_EQ(result_container_string.selectivity, 1.f / 3.f);
}

TEST_F(ColumnStatisticsTest, StoredProcedureBetweenTest) {
  ScanType scan_type = ScanType::OpBetween;

  // selectivities = selectivities from LessEqualThan / 0.3f

  std::vector<float> selectivities_int{0.f, 1.f / 18.f, 1.f / 6.f, 1.f / 3.f, 1.f / 3.f};
  predict_selectivities_for_stored_procedures_and_compare(_column_statistics_int, scan_type, _int_values,
                                                          selectivities_int);

  std::vector<float> selectivities_float{0.f, 0.f, 2.f / 15.f, 1.f / 3.f, 1.f / 3.f};
  predict_selectivities_for_stored_procedures_and_compare(_column_statistics_float, scan_type, _float_values,
                                                          selectivities_float);
  predict_selectivities_for_stored_procedures_and_compare(_column_statistics_double, scan_type, _double_values,
                                                          selectivities_float);
}

}  // namespace opossum
