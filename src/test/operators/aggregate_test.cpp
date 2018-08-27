#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_read_only_operator.hpp"
#include "operators/aggregate.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsAggregateTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper_1_1 = std::make_shared<TableWrapper>(
        load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2));
    _table_wrapper_1_1->execute();

    _table_wrapper_1_1_null = std::make_shared<TableWrapper>(
        load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 2));
    _table_wrapper_1_1_null->execute();

    _table_wrapper_1_2 = std::make_shared<TableWrapper>(
        load_table("src/test/tables/aggregateoperator/groupby_int_1gb_2agg/input.tbl", 2));
    _table_wrapper_1_2->execute();

    _table_wrapper_2_1 = std::make_shared<TableWrapper>(
        load_table("src/test/tables/aggregateoperator/groupby_int_2gb_1agg/input.tbl", 2));
    _table_wrapper_2_1->execute();

    _table_wrapper_2_2 = std::make_shared<TableWrapper>(
        load_table("src/test/tables/aggregateoperator/groupby_int_2gb_2agg/input.tbl", 2));
    _table_wrapper_2_2->execute();

    _table_wrapper_2_0_null = std::make_shared<TableWrapper>(
        load_table("src/test/tables/aggregateoperator/groupby_int_2gb_0agg/input_null.tbl", 2));
    _table_wrapper_2_0_null->execute();

    _table_wrapper_3_1 = std::make_shared<TableWrapper>(
        load_table("src/test/tables/aggregateoperator/groupby_int_3gb_1agg/input.tbl", 2));
    _table_wrapper_3_1->execute();

    _table_wrapper_3_0_null = std::make_shared<TableWrapper>(
        load_table("src/test/tables/aggregateoperator/groupby_int_3gb_0agg/input_null.tbl", 2));
    _table_wrapper_3_0_null->execute();

    _table_wrapper_1_1_string = std::make_shared<TableWrapper>(
        load_table("src/test/tables/aggregateoperator/groupby_string_1gb_1agg/input.tbl", 2));
    _table_wrapper_1_1_string->execute();

    _table_wrapper_1_1_string_null = std::make_shared<TableWrapper>(
        load_table("src/test/tables/aggregateoperator/groupby_string_1gb_1agg/input_null.tbl", 2));
    _table_wrapper_1_1_string_null->execute();

    _table_wrapper_join_1 = std::make_shared<TableWrapper>(load_table("src/test/tables/int4.tbl", 1));
    _table_wrapper_join_1->execute();

    _table_wrapper_join_2 = std::make_shared<TableWrapper>(load_table("src/test/tables/int.tbl", 1));
    _table_wrapper_join_2->execute();

    _table_wrapper_2_0_a =
        std::make_shared<TableWrapper>(load_table("src/test/tables/aggregateoperator/join_2gb_0agg/input_a.tbl", 2));
    _table_wrapper_2_0_a->execute();

    _table_wrapper_2_o_b =
        std::make_shared<TableWrapper>(load_table("src/test/tables/aggregateoperator/join_2gb_0agg/input_b.tbl", 2));
    _table_wrapper_2_o_b->execute();

    auto test_table = load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2);
    ChunkEncoder::encode_all_chunks(test_table);

    _table_wrapper_1_1_dict = std::make_shared<TableWrapper>(std::move(test_table));
    _table_wrapper_1_1_dict->execute();

    test_table = load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 2);
    ChunkEncoder::encode_all_chunks(test_table);

    _table_wrapper_1_1_null_dict = std::make_shared<TableWrapper>(std::move(test_table));
    _table_wrapper_1_1_null_dict->execute();

    _table_wrapper_int_int = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int.tbl", 2));
    _table_wrapper_int_int->execute();
  }

  void test_output(const std::shared_ptr<AbstractOperator> in, const std::vector<AggregateCxlumnDefinition>& aggregates,
                   const std::vector<CxlumnID>& groupby_cxlumn_ids, const std::string& file_name, size_t chunk_size,
                   bool test_aggregate_on_reference_table = true) {
    // load expected results from file
    std::shared_ptr<Table> expected_result = load_table(file_name, chunk_size);
    EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

    // Test the Aggregate on stored table data
    auto aggregate = std::make_shared<Aggregate>(in, aggregates, groupby_cxlumn_ids);
    aggregate->execute();
    EXPECT_TABLE_EQ_UNORDERED(aggregate->get_output(), expected_result);

    if (test_aggregate_on_reference_table) {
      // Perform a TableScan to create a reference table
      const auto table_scan = std::make_shared<TableScan>(in, CxlumnID{0}, PredicateCondition::GreaterThanEquals, 0);
      table_scan->execute();

      // Perform the Aggregate on a reference table
      const auto aggregate = std::make_shared<Aggregate>(table_scan, aggregates, groupby_cxlumn_ids);
      aggregate->execute();
      EXPECT_TABLE_EQ_UNORDERED(aggregate->get_output(), expected_result);
    }
  }

  std::shared_ptr<TableWrapper> _table_wrapper_1_1, _table_wrapper_1_1_null, _table_wrapper_join_1,
      _table_wrapper_join_2, _table_wrapper_1_2, _table_wrapper_2_1, _table_wrapper_2_2, _table_wrapper_2_0_null,
      _table_wrapper_3_1, _table_wrapper_3_2, _table_wrapper_3_0_null, _table_wrapper_1_1_string,
      _table_wrapper_1_1_string_null, _table_wrapper_1_1_dict, _table_wrapper_1_1_null_dict, _table_wrapper_2_0_a,
      _table_wrapper_2_o_b, _table_wrapper_int_int;
};

TEST_F(OperatorsAggregateTest, OperatorName) {
  auto aggregate = std::make_shared<Aggregate>(
      _table_wrapper_1_1, std::vector<AggregateCxlumnDefinition>{{CxlumnID{1}, AggregateFunction::Max}},
      std::vector<CxlumnID>{CxlumnID{0}});

  EXPECT_EQ(aggregate->name(), "Aggregate");
}

TEST_F(OperatorsAggregateTest, CannotSumStringColumns) {
  auto aggregate = std::make_shared<Aggregate>(
      _table_wrapper_1_1_string, std::vector<AggregateCxlumnDefinition>{{CxlumnID{0}, AggregateFunction::Sum}},
      std::vector<CxlumnID>{CxlumnID{0}});
  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TEST_F(OperatorsAggregateTest, CannotAvgStringColumns) {
  auto aggregate = std::make_shared<Aggregate>(
      _table_wrapper_1_1_string, std::vector<AggregateCxlumnDefinition>{{CxlumnID{0}, AggregateFunction::Avg}},
      std::vector<CxlumnID>{CxlumnID{0}});
  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TEST_F(OperatorsAggregateTest, CanCountStringColumns) {
  this->test_output(_table_wrapper_1_1_string, {{CxlumnID{0}, AggregateFunction::Count}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/count_str.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMax) {
  this->test_output(_table_wrapper_1_1, {{CxlumnID{1}, AggregateFunction::Max}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMin) {
  this->test_output(_table_wrapper_1_1, {{CxlumnID{1}, AggregateFunction::Min}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateSum) {
  this->test_output(_table_wrapper_1_1, {{CxlumnID{1}, AggregateFunction::Sum}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateAvg) {
  this->test_output(_table_wrapper_1_1, {{CxlumnID{1}, AggregateFunction::Avg}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateCount) {
  this->test_output(_table_wrapper_1_1, {{CxlumnID{1}, AggregateFunction::Count}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateCountDistinct) {
  this->test_output(_table_wrapper_1_1, {{CxlumnID{1}, AggregateFunction::CountDistinct}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count_distinct.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateMax) {
  this->test_output(_table_wrapper_1_1_string, {{CxlumnID{1}, AggregateFunction::Max}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateMin) {
  this->test_output(_table_wrapper_1_1_string, {{CxlumnID{1}, AggregateFunction::Min}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateStringMax) {
  this->test_output(_table_wrapper_1_1_string, {{CxlumnID{0}, AggregateFunction::Max}}, {},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/max_str.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateStringMin) {
  this->test_output(_table_wrapper_1_1_string, {{CxlumnID{0}, AggregateFunction::Min}}, {},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/min_str.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateSum) {
  this->test_output(_table_wrapper_1_1_string, {{CxlumnID{1}, AggregateFunction::Sum}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateAvg) {
  this->test_output(_table_wrapper_1_1_string, {{CxlumnID{1}, AggregateFunction::Avg}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateCount) {
  this->test_output(_table_wrapper_1_1_string, {{CxlumnID{1}, AggregateFunction::Count}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMax) {
  this->test_output(_table_wrapper_1_1_dict, {{CxlumnID{1}, AggregateFunction::Max}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMin) {
  this->test_output(_table_wrapper_1_1_dict, {{CxlumnID{1}, AggregateFunction::Min}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateSum) {
  this->test_output(_table_wrapper_1_1_dict, {{CxlumnID{1}, AggregateFunction::Sum}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateAvg) {
  this->test_output(_table_wrapper_1_1_dict, {{CxlumnID{1}, AggregateFunction::Avg}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateCount) {
  this->test_output(_table_wrapper_1_1_dict, {{CxlumnID{1}, AggregateFunction::Count}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateAvgMax) {
  this->test_output(_table_wrapper_1_2, {{CxlumnID{1}, AggregateFunction::Max}, {CxlumnID{2}, AggregateFunction::Avg}},
                    {CxlumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/max_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateMinAvg) {
  this->test_output(_table_wrapper_1_2, {{CxlumnID{1}, AggregateFunction::Min}, {CxlumnID{2}, AggregateFunction::Avg}},
                    {CxlumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/min_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateMinMax) {
  this->test_output(_table_wrapper_1_2, {{CxlumnID{1}, AggregateFunction::Min}, {CxlumnID{2}, AggregateFunction::Max}},
                    {CxlumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/min_max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateAvgAvg) {
  this->test_output(_table_wrapper_1_2, {{CxlumnID{1}, AggregateFunction::Avg}, {CxlumnID{2}, AggregateFunction::Avg}},
                    {CxlumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/avg_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumAvg) {
  this->test_output(_table_wrapper_1_2, {{CxlumnID{1}, AggregateFunction::Sum}, {CxlumnID{2}, AggregateFunction::Avg}},
                    {CxlumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumSum) {
  this->test_output(_table_wrapper_1_2, {{CxlumnID{1}, AggregateFunction::Sum}, {CxlumnID{2}, AggregateFunction::Sum}},
                    {CxlumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumCount) {
  this->test_output(_table_wrapper_1_2,
                    {{CxlumnID{1}, AggregateFunction::Sum}, {CxlumnID{2}, AggregateFunction::Count}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyMax) {
  this->test_output(_table_wrapper_2_1, {{CxlumnID{2}, AggregateFunction::Max}}, {CxlumnID{0}, CxlumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyMin) {
  this->test_output(_table_wrapper_2_1, {{CxlumnID{2}, AggregateFunction::Min}}, {CxlumnID{0}, CxlumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbySum) {
  this->test_output(_table_wrapper_2_1, {{CxlumnID{2}, AggregateFunction::Sum}}, {CxlumnID{0}, CxlumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAvg) {
  this->test_output(_table_wrapper_2_1, {{CxlumnID{2}, AggregateFunction::Avg}}, {CxlumnID{0}, CxlumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyCount) {
  this->test_output(_table_wrapper_2_1, {{CxlumnID{2}, AggregateFunction::Count}}, {CxlumnID{0}, CxlumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, ThreeGroupbyMax) {
  this->test_output(_table_wrapper_3_1, {{CxlumnID{2}, AggregateFunction::Max}},
                    {CxlumnID{0}, CxlumnID{1}, CxlumnID{3}},
                    "src/test/tables/aggregateoperator/groupby_int_3gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, ThreeGroupbyMin) {
  this->test_output(_table_wrapper_3_1, {{CxlumnID{2}, AggregateFunction::Min}},
                    {CxlumnID{0}, CxlumnID{1}, CxlumnID{3}},
                    "src/test/tables/aggregateoperator/groupby_int_3gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, ThreeGroupbySum) {
  this->test_output(_table_wrapper_3_1, {{CxlumnID{2}, AggregateFunction::Sum}},
                    {CxlumnID{0}, CxlumnID{1}, CxlumnID{3}},
                    "src/test/tables/aggregateoperator/groupby_int_3gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, ThreeGroupbyAvg) {
  this->test_output(_table_wrapper_3_1, {{CxlumnID{2}, AggregateFunction::Avg}},
                    {CxlumnID{0}, CxlumnID{1}, CxlumnID{3}},
                    "src/test/tables/aggregateoperator/groupby_int_3gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, ThreeGroupbyCount) {
  this->test_output(_table_wrapper_3_1, {{CxlumnID{2}, AggregateFunction::Count}},
                    {CxlumnID{0}, CxlumnID{1}, CxlumnID{3}},
                    "src/test/tables/aggregateoperator/groupby_int_3gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMaxAvg) {
  this->test_output(_table_wrapper_2_2, {{CxlumnID{2}, AggregateFunction::Max}, {CxlumnID{3}, AggregateFunction::Avg}},
                    {CxlumnID{0}, CxlumnID{1}}, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl",
                    1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvg) {
  this->test_output(_table_wrapper_2_2, {{CxlumnID{2}, AggregateFunction::Min}, {CxlumnID{3}, AggregateFunction::Avg}},
                    {CxlumnID{0}, CxlumnID{1}}, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/min_avg.tbl",
                    1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinMax) {
  this->test_output(_table_wrapper_2_2, {{CxlumnID{2}, AggregateFunction::Min}, {CxlumnID{3}, AggregateFunction::Max}},
                    {CxlumnID{0}, CxlumnID{1}}, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/min_max.tbl",
                    1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumAvg) {
  this->test_output(_table_wrapper_2_2, {{CxlumnID{2}, AggregateFunction::Sum}, {CxlumnID{3}, AggregateFunction::Avg}},
                    {CxlumnID{0}, CxlumnID{1}}, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/sum_avg.tbl",
                    1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumSum) {
  this->test_output(_table_wrapper_2_2, {{CxlumnID{2}, AggregateFunction::Sum}, {CxlumnID{3}, AggregateFunction::Sum}},
                    {CxlumnID{0}, CxlumnID{1}}, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/sum_sum.tbl",
                    1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumCount) {
  this->test_output(
      _table_wrapper_2_2, {{CxlumnID{2}, AggregateFunction::Sum}, {CxlumnID{3}, AggregateFunction::Count}},
      {CxlumnID{0}, CxlumnID{1}}, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/sum_count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateMax) {
  this->test_output(_table_wrapper_1_1, {{CxlumnID{1}, AggregateFunction::Max}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateMin) {
  this->test_output(_table_wrapper_1_1, {{CxlumnID{1}, AggregateFunction::Min}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateSum) {
  this->test_output(_table_wrapper_1_1, {{CxlumnID{1}, AggregateFunction::Sum}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateAvg) {
  this->test_output(_table_wrapper_1_1, {{CxlumnID{1}, AggregateFunction::Avg}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateCount) {
  this->test_output(_table_wrapper_1_1, {{CxlumnID{1}, AggregateFunction::Count}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, OneGroupbyAndNoAggregate) {
  this->test_output(_table_wrapper_1_1, {}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_0agg/result.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndNoAggregate) {
  this->test_output(_table_wrapper_1_1, {}, {CxlumnID{0}, CxlumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_0agg/result.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbyAndNoAggregate) {
  EXPECT_THROW(std::make_shared<Aggregate>(_table_wrapper_1_1_string, std::vector<AggregateCxlumnDefinition>{},
                                           std::vector<CxlumnID>{}),
               std::logic_error);
}

/**
 * Tests for NULL values
 */
TEST_F(OperatorsAggregateTest, CanCountStringColumnsWithNull) {
  this->test_output(_table_wrapper_1_1_string_null, {{CxlumnID{1}, AggregateFunction::Count}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/count_str_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMaxWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{CxlumnID{1}, AggregateFunction::Max}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMinWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{CxlumnID{1}, AggregateFunction::Min}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, SingleAggregateSumWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{CxlumnID{1}, AggregateFunction::Sum}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, SingleAggregateAvgWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{CxlumnID{1}, AggregateFunction::Avg}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, SingleAggregateCountWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{CxlumnID{1}, AggregateFunction::Count}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, OneGroupbyAndNoAggregateWithNull) {
  this->test_output(_table_wrapper_1_1_null, {}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_0agg/result_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, OneGroupbyCountStar) {
  this->test_output(_table_wrapper_1_1_null, {{std::nullopt, AggregateFunction::Count}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_0agg/count_star.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyCountStar) {
  this->test_output(_table_wrapper_2_0_null, {{std::nullopt, AggregateFunction::Count}}, {CxlumnID{0}, CxlumnID{2}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_0agg/count_star.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, ThreeGroupbyCountStar) {
  this->test_output(_table_wrapper_3_0_null, {{std::nullopt, AggregateFunction::Count}},
                    {CxlumnID{0}, CxlumnID{2}, CxlumnID{3}},
                    "src/test/tables/aggregateoperator/groupby_int_3gb_0agg/count_star.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMaxWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{CxlumnID{1}, AggregateFunction::Max}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMinWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{CxlumnID{1}, AggregateFunction::Min}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateSumWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{CxlumnID{1}, AggregateFunction::Sum}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateAvgWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{CxlumnID{1}, AggregateFunction::Avg}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateCountWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{CxlumnID{1}, AggregateFunction::Count}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count_null.tbl", 1, false);
}

/**
 * Tests for empty tables
 */

TEST_F(OperatorsAggregateTest, TwoAggregateEmptyTable) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_1_2, CxlumnID{0}, PredicateCondition::LessThan, 0);
  filtered->execute();
  this->test_output(filtered,
                    {{CxlumnID{1}, AggregateFunction::Max},
                     {CxlumnID{2}, AggregateFunction::Count},
                     {std::nullopt, AggregateFunction::Count}},
                    {}, "src/test/tables/aggregateoperator/0gb_3agg/max_count_count_empty.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateEmptyTableGrouped) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_1_2, CxlumnID{0}, PredicateCondition::LessThan, 0);
  filtered->execute();
  this->test_output(filtered,
                    {{CxlumnID{1}, AggregateFunction::Max},
                     {CxlumnID{2}, AggregateFunction::Count},
                     {std::nullopt, AggregateFunction::Count}},
                    {CxlumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_3agg/max_count_count_empty.tbl",
                    1);
}

/**
 * Tests for ReferenceSegments
 */

TEST_F(OperatorsAggregateTest, SingleAggregateMaxOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_1_1, CxlumnID{0}, PredicateCondition::LessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{CxlumnID{1}, AggregateFunction::Max}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_2_2, CxlumnID{0}, PredicateCondition::LessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{CxlumnID{2}, AggregateFunction::Min}, {CxlumnID{3}, AggregateFunction::Avg}},
                    {CxlumnID{0}, CxlumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/min_avg_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbySumOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_2_1, CxlumnID{0}, PredicateCondition::LessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{CxlumnID{2}, AggregateFunction::Sum}}, {CxlumnID{0}, CxlumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/sum_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_1_2, CxlumnID{0}, PredicateCondition::LessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{CxlumnID{1}, AggregateFunction::Sum}, {CxlumnID{2}, AggregateFunction::Avg}},
                    {CxlumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_avg_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMinOnRef) {
  auto filtered =
      std::make_shared<TableScan>(_table_wrapper_1_1_dict, CxlumnID{0}, PredicateCondition::LessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{CxlumnID{1}, AggregateFunction::Min}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, JoinThenAggregate) {
  auto join = std::make_shared<JoinHash>(_table_wrapper_2_0_a, _table_wrapper_2_o_b, JoinMode::Inner,
                                         CxlumnIDPair(CxlumnID{0}, CxlumnID{0}), PredicateCondition::Equals);
  join->execute();

  this->test_output(join, {}, {CxlumnID{0}, CxlumnID{3}}, "src/test/tables/aggregateoperator/join_2gb_0agg/result.tbl",
                    1);
}

TEST_F(OperatorsAggregateTest, OuterJoinThenAggregate) {
  auto join = std::make_shared<JoinNestedLoop>(_table_wrapper_join_1, _table_wrapper_join_2, JoinMode::Outer,
                                               CxlumnIDPair(CxlumnID{0}, CxlumnID{0}), PredicateCondition::LessThan);
  join->execute();

  this->test_output(join, {{CxlumnID{1}, AggregateFunction::Min}}, {CxlumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/outer_join.tbl", 1, false);
}

}  // namespace opossum
