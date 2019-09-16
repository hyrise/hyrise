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

#include "operators/abstract_read_only_operator.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class OperatorsAggregateTest : public BaseTest {
 public:
  static void SetUpTestCase() {  // called ONCE before the tests
    _table_wrapper_1_1 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2));
    _table_wrapper_1_1->execute();

    _table_wrapper_1_1_large = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input_large.tbl", 2));
    _table_wrapper_1_1_large->execute();

    _table_wrapper_1_1_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 2));
    _table_wrapper_1_1_null->execute();

    _table_wrapper_1_2 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/input.tbl", 2));
    _table_wrapper_1_2->execute();

    _table_wrapper_2_1 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/input.tbl", 2));
    _table_wrapper_2_1->execute();

    _table_wrapper_2_2 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/input.tbl", 2));
    _table_wrapper_2_2->execute();

    _table_wrapper_2_0_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_2gb_0agg/input_null.tbl", 2));
    _table_wrapper_2_0_null->execute();

    _table_wrapper_3_1 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/input.tbl", 2));
    _table_wrapper_3_1->execute();

    _table_wrapper_3_0_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_3gb_0agg/input_null.tbl", 2));
    _table_wrapper_3_0_null->execute();

    _table_wrapper_1_1_string = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/input.tbl", 2));
    _table_wrapper_1_1_string->execute();

    _table_wrapper_1_1_string_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/input_null.tbl", 2));
    _table_wrapper_1_1_string_null->execute();

    _table_wrapper_join_1 = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int4.tbl", 1));
    _table_wrapper_join_1->execute();

    _table_wrapper_join_2 = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int.tbl", 1));
    _table_wrapper_join_2->execute();

    _table_wrapper_2_0_a = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/join_2gb_0agg/input_a.tbl", 2));
    _table_wrapper_2_0_a->execute();

    _table_wrapper_2_o_b = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/join_2gb_0agg/input_b.tbl", 2));
    _table_wrapper_2_o_b->execute();

    auto test_table = load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2);
    ChunkEncoder::encode_all_chunks(test_table);

    _table_wrapper_1_1_dict = std::make_shared<TableWrapper>(std::move(test_table));
    _table_wrapper_1_1_dict->execute();

    test_table = load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 2);
    ChunkEncoder::encode_all_chunks(test_table);

    _table_wrapper_1_1_null_dict = std::make_shared<TableWrapper>(std::move(test_table));
    _table_wrapper_1_1_null_dict->execute();

    _table_wrapper_int_int = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int.tbl", 2));
    _table_wrapper_int_int->execute();
  }

 protected:
  void SetUp() override {}

  void test_output(const std::shared_ptr<AbstractOperator> in, const std::vector<AggregateColumnDefinition>& aggregates,
                   const std::vector<ColumnID>& groupby_column_ids, const std::string& file_name, size_t chunk_size,
                   bool test_aggregate_on_reference_table = true) {
    // load expected results from file
    std::shared_ptr<Table> expected_result = load_table(file_name, chunk_size);
    EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

    {
      // Test the Aggregate on stored table data
      auto aggregate = std::make_shared<T>(in, aggregates, groupby_column_ids);
      aggregate->execute();
      EXPECT_TABLE_EQ_UNORDERED(aggregate->get_output(), expected_result);
    }

    if (test_aggregate_on_reference_table) {
      // Perform a TableScan to create a reference table
      const auto table_scan = std::make_shared<TableScan>(in, greater_than_(get_column_expression(in, ColumnID{0}), 0));
      table_scan->execute();

      // Perform the Aggregate on a reference table
      const auto aggregate = std::make_shared<T>(table_scan, aggregates, groupby_column_ids);
      aggregate->execute();
      EXPECT_TABLE_EQ_UNORDERED(aggregate->get_output(), expected_result);
    }
  }

  inline static std::shared_ptr<TableWrapper> _table_wrapper_1_1, _table_wrapper_1_1_null, _table_wrapper_1_1_large,
      _table_wrapper_join_1, _table_wrapper_join_2, _table_wrapper_1_2, _table_wrapper_2_1, _table_wrapper_2_2,
      _table_wrapper_2_0_null, _table_wrapper_3_1, _table_wrapper_3_2, _table_wrapper_3_0_null,
      _table_wrapper_1_1_string, _table_wrapper_1_1_string_null, _table_wrapper_1_1_dict, _table_wrapper_1_1_null_dict,
      _table_wrapper_2_0_a, _table_wrapper_2_o_b, _table_wrapper_int_int;
};

using AggregateTypes = ::testing::Types<AggregateHash, AggregateSort>;
TYPED_TEST_SUITE(OperatorsAggregateTest, AggregateTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(OperatorsAggregateTest, OperatorName) {
  auto aggregate = std::make_shared<TypeParam>(
      this->_table_wrapper_1_1, std::vector<AggregateColumnDefinition>{{ColumnID{1}, AggregateFunction::Max}},
      std::vector<ColumnID>{ColumnID{0}});

  if constexpr (std::is_same_v<TypeParam, AggregateHash>) {
    EXPECT_EQ(aggregate->name(), "Aggregate");
  } else if constexpr (std::is_same_v<TypeParam, AggregateSort>) {
    EXPECT_EQ(aggregate->name(), "AggregateSort");
  } else {
    Fail("Unknown aggregate type");
  }
}

TYPED_TEST(OperatorsAggregateTest, CannotSumStringColumns) {
  auto aggregate = std::make_shared<TypeParam>(
      this->_table_wrapper_1_1_string, std::vector<AggregateColumnDefinition>{{ColumnID{0}, AggregateFunction::Sum}},
      std::vector<ColumnID>{ColumnID{0}});
  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TYPED_TEST(OperatorsAggregateTest, CannotAvgStringColumns) {
  auto aggregate = std::make_shared<TypeParam>(
      this->_table_wrapper_1_1_string, std::vector<AggregateColumnDefinition>{{ColumnID{0}, AggregateFunction::Avg}},
      std::vector<ColumnID>{ColumnID{0}});
  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TYPED_TEST(OperatorsAggregateTest, CannotStandardDeviationSampleStringColumns) {
  auto aggregate = std::make_shared<TypeParam>(
      this->_table_wrapper_1_1_string,
      std::vector<AggregateColumnDefinition>{{ColumnID{0}, AggregateFunction::StandardDeviationSample}},
      std::vector<ColumnID>{ColumnID{0}});
  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TYPED_TEST(OperatorsAggregateTest, CanCountStringColumns) {
  this->test_output(this->_table_wrapper_1_1_string, {{ColumnID{0}, AggregateFunction::Count}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/count_str.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMax) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMin) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateSum) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateAvg) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateStandardDeviationSample) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}},
                    {ColumnID{0}}, "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateStandardDeviationSampleLarge) {
  this->test_output(this->_table_wrapper_1_1_large, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}},
                    {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp_large.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateCount) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateCountDistinct) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::CountDistinct}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count_distinct.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateMax) {
  this->test_output(this->_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/max.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateMin) {
  this->test_output(this->_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/min.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateStringMax) {
  this->test_output(this->_table_wrapper_1_1_string, {{ColumnID{0}, AggregateFunction::Max}}, {},
                    "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/max_str.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateStringMin) {
  this->test_output(this->_table_wrapper_1_1_string, {{ColumnID{0}, AggregateFunction::Min}}, {},
                    "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/min_str.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateSum) {
  this->test_output(this->_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/sum.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateAvg) {
  this->test_output(this->_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateStandardDeviationSample) {
  this->test_output(this->_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}},
                    {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/stddev_samp_null.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateCount) {
  this->test_output(this->_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/count.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMax) {
  this->test_output(this->_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMin) {
  this->test_output(this->_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateSum) {
  this->test_output(this->_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateAvg) {
  this->test_output(this->_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateCount) {
  this->test_output(this->_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateAvgMax) {
  this->test_output(this->_table_wrapper_1_2,
                    {{ColumnID{1}, AggregateFunction::Max}, {ColumnID{2}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/max_avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateMaxStandardDeviationSample) {
  this->test_output(this->_table_wrapper_1_2,
                    {{ColumnID{1}, AggregateFunction::Max}, {ColumnID{2}, AggregateFunction::StandardDeviationSample}},
                    {ColumnID{0}}, "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/max_stddev_samp.tbl",
                    1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateMinAvg) {
  this->test_output(this->_table_wrapper_1_2,
                    {{ColumnID{1}, AggregateFunction::Min}, {ColumnID{2}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/min_avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateMinStandardDeviationSample) {
  this->test_output(this->_table_wrapper_1_2,
                    {{ColumnID{1}, AggregateFunction::Min}, {ColumnID{2}, AggregateFunction::StandardDeviationSample}},
                    {ColumnID{0}}, "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/min_stddev_samp.tbl",
                    1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateMinMax) {
  this->test_output(this->_table_wrapper_1_2,
                    {{ColumnID{1}, AggregateFunction::Min}, {ColumnID{2}, AggregateFunction::Max}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/min_max.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateAvgAvg) {
  this->test_output(this->_table_wrapper_1_2,
                    {{ColumnID{1}, AggregateFunction::Avg}, {ColumnID{2}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/avg_avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateStandardDeviationSampleStandardDeviationSample) {
  this->test_output(this->_table_wrapper_1_2,
                    {{ColumnID{1}, AggregateFunction::StandardDeviationSample},
                     {ColumnID{2}, AggregateFunction::StandardDeviationSample}},
                    {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/stddev_samp_stddev_samp.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateSumAvg) {
  this->test_output(this->_table_wrapper_1_2,
                    {{ColumnID{1}, AggregateFunction::Sum}, {ColumnID{2}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/sum_avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateSumSum) {
  this->test_output(this->_table_wrapper_1_2,
                    {{ColumnID{1}, AggregateFunction::Sum}, {ColumnID{2}, AggregateFunction::Sum}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/sum_sum.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateSumCount) {
  this->test_output(this->_table_wrapper_1_2,
                    {{ColumnID{1}, AggregateFunction::Sum}, {ColumnID{2}, AggregateFunction::Count}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/sum_count.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyMax) {
  this->test_output(this->_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Max}}, {ColumnID{0}, ColumnID{1}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/max.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyMin) {
  this->test_output(this->_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Min}}, {ColumnID{0}, ColumnID{1}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/min.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbySum) {
  this->test_output(this->_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Sum}}, {ColumnID{0}, ColumnID{1}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/sum.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAvg) {
  this->test_output(this->_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Avg}}, {ColumnID{0}, ColumnID{1}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyStandardDeviationSample) {
  this->test_output(this->_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::StandardDeviationSample}},
                    {ColumnID{0}, ColumnID{1}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/stddev_samp_null.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyCount) {
  this->test_output(this->_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Count}}, {ColumnID{0}, ColumnID{1}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/count.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyMax) {
  this->test_output(this->_table_wrapper_3_1, {{ColumnID{2}, AggregateFunction::Max}},
                    {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/max.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyMin) {
  this->test_output(this->_table_wrapper_3_1, {{ColumnID{2}, AggregateFunction::Min}},
                    {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/min.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbySum) {
  this->test_output(this->_table_wrapper_3_1, {{ColumnID{2}, AggregateFunction::Sum}},
                    {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/sum.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyAvg) {
  this->test_output(this->_table_wrapper_3_1, {{ColumnID{2}, AggregateFunction::Avg}},
                    {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyStandardDeviationSample) {
  this->test_output(this->_table_wrapper_3_1, {{ColumnID{2}, AggregateFunction::StandardDeviationSample}},
                    {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/stddev_samp_null.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyCount) {
  this->test_output(this->_table_wrapper_3_1, {{ColumnID{2}, AggregateFunction::Count}},
                    {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/count.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMaxAvg) {
  this->test_output(
      this->_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Max}, {ColumnID{3}, AggregateFunction::Avg}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvg) {
  this->test_output(
      this->_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Min}, {ColumnID{3}, AggregateFunction::Avg}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/min_avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinMax) {
  this->test_output(
      this->_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Min}, {ColumnID{3}, AggregateFunction::Max}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/min_max.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumAvg) {
  this->test_output(
      this->_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Sum}, {ColumnID{3}, AggregateFunction::Avg}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/sum_avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateStandardDeviationSampleAvg) {
  this->test_output(this->_table_wrapper_2_2,
                    {{ColumnID{2}, AggregateFunction::StandardDeviationSample}, {ColumnID{3}, AggregateFunction::Avg}},
                    {ColumnID{0}, ColumnID{1}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/stddev_samp_avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateStandardDeviationSampleStandardDeviationSample) {
  this->test_output(this->_table_wrapper_2_2,
                    {{ColumnID{2}, AggregateFunction::StandardDeviationSample},
                     {ColumnID{3}, AggregateFunction::StandardDeviationSample}},
                    {ColumnID{0}, ColumnID{1}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/stddev_samp_stddev_samp.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumSum) {
  this->test_output(
      this->_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Sum}, {ColumnID{3}, AggregateFunction::Sum}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/sum_sum.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumCount) {
  this->test_output(
      this->_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Sum}, {ColumnID{3}, AggregateFunction::Count}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/sum_count.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateMax) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Max}}, {},
                    "resources/test_data/tbl/aggregateoperator/0gb_1agg/max.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateMin) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Min}}, {},
                    "resources/test_data/tbl/aggregateoperator/0gb_1agg/min.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateSum) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Sum}}, {},
                    "resources/test_data/tbl/aggregateoperator/0gb_1agg/sum.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateAvg) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Avg}}, {},
                    "resources/test_data/tbl/aggregateoperator/0gb_1agg/avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateStandardDeviationSample) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}}, {},
                    "resources/test_data/tbl/aggregateoperator/0gb_1agg/stddev_samp.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateCount) {
  this->test_output(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Count}}, {},
                    "resources/test_data/tbl/aggregateoperator/0gb_1agg/count.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, OneGroupbyAndNoAggregate) {
  this->test_output(this->_table_wrapper_1_1, {}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/result.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndNoAggregate) {
  this->test_output(this->_table_wrapper_1_1, {}, {ColumnID{0}, ColumnID{1}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_0agg/result.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbyAndNoAggregate) {
  EXPECT_THROW(std::make_shared<TypeParam>(this->_table_wrapper_1_1_string, std::vector<AggregateColumnDefinition>{},
                                           std::vector<ColumnID>{}),
               std::logic_error);
}

/**
 * Tests for NULL values
 */
TYPED_TEST(OperatorsAggregateTest, CanCountStringColumnsWithNull) {
  this->test_output(this->_table_wrapper_1_1_string_null, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/count_str_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMaxWithNull) {
  this->test_output(this->_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMinWithNull) {
  this->test_output(this->_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateSumWithNull) {
  this->test_output(this->_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateAvgWithNull) {
  this->test_output(this->_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateStandardDeviationSampleWithNull) {
  this->test_output(this->_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}},
                    {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateCountWithNull) {
  this->test_output(this->_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, OneGroupbyAndNoAggregateWithNull) {
  this->test_output(this->_table_wrapper_1_1_null, {}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/result_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, OneGroupbyCountStar) {
  this->test_output(this->_table_wrapper_1_1_null, {{std::nullopt, AggregateFunction::Count}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/count_star.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyCountStar) {
  this->test_output(this->_table_wrapper_2_0_null, {{std::nullopt, AggregateFunction::Count}},
                    {ColumnID{0}, ColumnID{2}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_0agg/count_star.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyCountStar) {
  this->test_output(this->_table_wrapper_3_0_null, {{std::nullopt, AggregateFunction::Count}},
                    {ColumnID{0}, ColumnID{2}, ColumnID{3}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_0agg/count_star.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMaxWithNull) {
  this->test_output(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMinWithNull) {
  this->test_output(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateSumWithNull) {
  this->test_output(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateAvgWithNull) {
  this->test_output(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateStandardDeviationSampleWithNull) {
  this->test_output(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}},
                    {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp_null.tbl", 1, false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateCountWithNull) {
  this->test_output(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count_null.tbl", 1, false);
}

/**
 * Tests for empty tables
 */

TYPED_TEST(OperatorsAggregateTest, TwoAggregateEmptyTable) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_2, less_than_(this->get_column_expression(this->_table_wrapper_1_2, ColumnID{0}), 0));
  filtered->execute();
  this->test_output(filtered,
                    {{ColumnID{1}, AggregateFunction::Max},
                     {ColumnID{2}, AggregateFunction::Count},
                     {std::nullopt, AggregateFunction::Count}},
                    {}, "resources/test_data/tbl/aggregateoperator/0gb_3agg/max_count_count_empty.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateEmptyTableGrouped) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_2, less_than_(this->get_column_expression(this->_table_wrapper_1_2, ColumnID{0}), 0));
  filtered->execute();
  this->test_output(filtered,
                    {{ColumnID{1}, AggregateFunction::Max},
                     {ColumnID{2}, AggregateFunction::Count},
                     {std::nullopt, AggregateFunction::Count}},
                    {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_3agg/max_count_count_empty.tbl", 1);
}

/**
 * Tests for ReferenceSegments
 */

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMaxOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_1, less_than_(this->get_column_expression(this->_table_wrapper_1_1, ColumnID{0}), "100"));
  filtered->execute();

  this->test_output(filtered, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max_filtered.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_2_2, less_than_(this->get_column_expression(this->_table_wrapper_2_2, ColumnID{0}), "100"));
  filtered->execute();

  this->test_output(filtered, {{ColumnID{2}, AggregateFunction::Min}, {ColumnID{3}, AggregateFunction::Avg}},
                    {ColumnID{0}, ColumnID{1}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/min_avg_filtered.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbySumOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_2_1, less_than_(this->get_column_expression(this->_table_wrapper_2_1, ColumnID{0}), "100"));
  filtered->execute();

  this->test_output(filtered, {{ColumnID{2}, AggregateFunction::Sum}}, {ColumnID{0}, ColumnID{1}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/sum_filtered.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateSumAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_2, less_than_(this->get_column_expression(this->_table_wrapper_1_2, ColumnID{0}), "100"));
  filtered->execute();

  this->test_output(filtered, {{ColumnID{1}, AggregateFunction::Sum}, {ColumnID{2}, AggregateFunction::Avg}},
                    {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/sum_avg_filtered.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateStandardDeviationSampleAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_2, less_than_(this->get_column_expression(this->_table_wrapper_1_2, ColumnID{0}), "100"));
  filtered->execute();

  this->test_output(
      filtered, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}, {ColumnID{2}, AggregateFunction::Avg}},
      {ColumnID{0}}, "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/stddev_samp_avg_filtered.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMinOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_1_dict,
      less_than_(this->get_column_expression(this->_table_wrapper_1_1_dict, ColumnID{0}), "100"));
  filtered->execute();

  this->test_output(filtered, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min_filtered.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateStandardDeviationSampleOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_1_dict,
      less_than_(this->get_column_expression(this->_table_wrapper_1_1_dict, ColumnID{0}), "100"));
  filtered->execute();

  this->test_output(filtered, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp_filtered.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, JoinThenAggregate) {
  auto join = std::make_shared<JoinHash>(
      this->_table_wrapper_2_0_a, this->_table_wrapper_2_o_b, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
  join->execute();

  this->test_output(join, {}, {ColumnID{0}, ColumnID{3}},
                    "resources/test_data/tbl/aggregateoperator/join_2gb_0agg/result.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, OuterJoinThenAggregate) {
  auto join =
      std::make_shared<JoinNestedLoop>(this->_table_wrapper_join_1, this->_table_wrapper_join_2, JoinMode::FullOuter,
                                       OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThan});
  join->execute();

  this->test_output(join, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/outer_join.tbl", 1, false);
}

}  // namespace opossum
