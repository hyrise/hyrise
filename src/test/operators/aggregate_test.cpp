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
#include "storage/dictionary_compression.hpp"
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

    _table_wrapper_3_1 =
        std::make_shared<TableWrapper>(load_table("src/test/tables/aggregateoperator/join_2gb_0agg/input_a.tbl", 2));
    _table_wrapper_3_1->execute();

    _table_wrapper_3_2 =
        std::make_shared<TableWrapper>(load_table("src/test/tables/aggregateoperator/join_2gb_0agg/input_b.tbl", 2));
    _table_wrapper_3_2->execute();

    auto test_table = load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2);
    DictionaryCompression::compress_table(*test_table);

    _table_wrapper_1_1_dict = std::make_shared<TableWrapper>(std::move(test_table));
    _table_wrapper_1_1_dict->execute();

    test_table = load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 2);
    DictionaryCompression::compress_table(*test_table);

    _table_wrapper_1_1_null_dict = std::make_shared<TableWrapper>(std::move(test_table));
    _table_wrapper_1_1_null_dict->execute();
  }

  void test_output(const std::shared_ptr<AbstractOperator> in, const std::vector<AggregateDefinition>& aggregates,
                   const std::vector<ColumnID>& groupby_column_ids, const std::string& file_name, size_t chunk_size,
                   bool test_references = true) {
    // load expected results from file
    std::shared_ptr<Table> expected_result = load_table(file_name, chunk_size);
    EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

    // collect possible columns to scan before aggregate
    std::set<ColumnID> ref_columns;

    // this means no prior table scan
    ref_columns.insert(INVALID_COLUMN_ID);

    if (test_references) {
      for (const auto& agg : aggregates) {
        ref_columns.insert(agg.column_id);
      }

      for (const auto column_id : groupby_column_ids) {
        ref_columns.insert(column_id);
      }
    }

    EXPECT_NE(ref_columns.size(), 0u);

    for (auto& ref : ref_columns) {
      // make one Aggregate w/o ReferenceColumn
      auto input = in;

      if (ref != INVALID_COLUMN_ID) {
        // also try a TableScan on every involved column
        input = std::make_shared<TableScan>(in, ref, ScanType::GreaterThanEquals, 0);
        input->execute();
      }

      // build and execute Aggregate
      auto aggregate = std::make_shared<Aggregate>(input, aggregates, groupby_column_ids);
      EXPECT_NE(aggregate, nullptr) << "Could not build Aggregate";
      aggregate->execute();
      EXPECT_TABLE_EQ_UNORDERED(aggregate->get_output(), expected_result);
    }
  }

  std::shared_ptr<TableWrapper> _table_wrapper_1_1, _table_wrapper_1_1_null, _table_wrapper_join_1,
      _table_wrapper_join_2, _table_wrapper_1_2, _table_wrapper_2_1, _table_wrapper_2_2, _table_wrapper_2_0_null,
      _table_wrapper_1_1_string, _table_wrapper_1_1_string_null, _table_wrapper_1_1_dict, _table_wrapper_1_1_null_dict,
      _table_wrapper_3_1, _table_wrapper_3_2;
};

TEST_F(OperatorsAggregateTest, OperatorName) {
  auto aggregate = std::make_shared<Aggregate>(_table_wrapper_1_1,
                                               std::vector<AggregateDefinition>{{ColumnID{1}, AggregateFunction::Max}},
                                               std::vector<ColumnID>{ColumnID{0}});

  EXPECT_EQ(aggregate->name(), "Aggregate");
}

TEST_F(OperatorsAggregateTest, CannotSumStringColumns) {
  auto aggregate = std::make_shared<Aggregate>(_table_wrapper_1_1_string,
                                               std::vector<AggregateDefinition>{{ColumnID{0}, AggregateFunction::Sum}},
                                               std::vector<ColumnID>{ColumnID{0}});
  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TEST_F(OperatorsAggregateTest, CannotAvgStringColumns) {
  auto aggregate = std::make_shared<Aggregate>(_table_wrapper_1_1_string,
                                               std::vector<AggregateDefinition>{{ColumnID{0}, AggregateFunction::Avg}},
                                               std::vector<ColumnID>{ColumnID{0}});
  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TEST_F(OperatorsAggregateTest, CanCountStringColumns) {
  this->test_output(_table_wrapper_1_1_string, {{ColumnID{0}, AggregateFunction::Count}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/count_str.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMax) {
  this->test_output(_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMin) {
  this->test_output(_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateSum) {
  this->test_output(_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateAvg) {
  this->test_output(_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateCount) {
  this->test_output(_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateCountDistinct) {
  this->test_output(_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::CountDistinct}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count_distinct.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateMax) {
  this->test_output(_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateMin) {
  this->test_output(_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateStringMax) {
  this->test_output(_table_wrapper_1_1_string, {{ColumnID{0}, AggregateFunction::Max}}, {},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/max_str.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateStringMin) {
  this->test_output(_table_wrapper_1_1_string, {{ColumnID{0}, AggregateFunction::Min}}, {},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/min_str.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateSum) {
  this->test_output(_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateAvg) {
  this->test_output(_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateCount) {
  this->test_output(_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMax) {
  this->test_output(_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMin) {
  this->test_output(_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateSum) {
  this->test_output(_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateAvg) {
  this->test_output(_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateCount) {
  this->test_output(_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateAvgMax) {
  this->test_output(_table_wrapper_1_2, {{ColumnID{1}, AggregateFunction::Max}, {ColumnID{2}, AggregateFunction::Avg}},
                    {ColumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/max_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateMinAvg) {
  this->test_output(_table_wrapper_1_2, {{ColumnID{1}, AggregateFunction::Min}, {ColumnID{2}, AggregateFunction::Avg}},
                    {ColumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/min_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateMinMax) {
  this->test_output(_table_wrapper_1_2, {{ColumnID{1}, AggregateFunction::Min}, {ColumnID{2}, AggregateFunction::Max}},
                    {ColumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/min_max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateAvgAvg) {
  this->test_output(_table_wrapper_1_2, {{ColumnID{1}, AggregateFunction::Avg}, {ColumnID{2}, AggregateFunction::Avg}},
                    {ColumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/avg_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumAvg) {
  this->test_output(_table_wrapper_1_2, {{ColumnID{1}, AggregateFunction::Sum}, {ColumnID{2}, AggregateFunction::Avg}},
                    {ColumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumAvgAlias) {
  this->test_output(_table_wrapper_1_2, {{ColumnID{1}, AggregateFunction::Sum, std::optional<std::string>("sum_b")},
                                         {ColumnID{2}, AggregateFunction::Avg}},
                    {ColumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_avg_alias.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumSum) {
  this->test_output(_table_wrapper_1_2, {{ColumnID{1}, AggregateFunction::Sum}, {ColumnID{2}, AggregateFunction::Sum}},
                    {ColumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumCount) {
  this->test_output(_table_wrapper_1_2,
                    {{ColumnID{1}, AggregateFunction::Sum}, {ColumnID{2}, AggregateFunction::Count}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyMax) {
  this->test_output(_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Max}}, {ColumnID{0}, ColumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyMin) {
  this->test_output(_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Min}}, {ColumnID{0}, ColumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbySum) {
  this->test_output(_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Sum}}, {ColumnID{0}, ColumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAvg) {
  this->test_output(_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Avg}}, {ColumnID{0}, ColumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyCount) {
  this->test_output(_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Count}}, {ColumnID{0}, ColumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMaxAvg) {
  this->test_output(_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Max}, {ColumnID{3}, AggregateFunction::Avg}},
                    {ColumnID{0}, ColumnID{1}}, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl",
                    1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvg) {
  this->test_output(_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Min}, {ColumnID{3}, AggregateFunction::Avg}},
                    {ColumnID{0}, ColumnID{1}}, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/min_avg.tbl",
                    1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinMax) {
  this->test_output(_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Min}, {ColumnID{3}, AggregateFunction::Max}},
                    {ColumnID{0}, ColumnID{1}}, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/min_max.tbl",
                    1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumAvg) {
  this->test_output(_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Sum}, {ColumnID{3}, AggregateFunction::Avg}},
                    {ColumnID{0}, ColumnID{1}}, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/sum_avg.tbl",
                    1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumSum) {
  this->test_output(_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Sum}, {ColumnID{3}, AggregateFunction::Sum}},
                    {ColumnID{0}, ColumnID{1}}, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/sum_sum.tbl",
                    1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumCount) {
  this->test_output(
      _table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Sum}, {ColumnID{3}, AggregateFunction::Count}},
      {ColumnID{0}, ColumnID{1}}, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/sum_count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateMax) {
  this->test_output(_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Max}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateMin) {
  this->test_output(_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Min}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateSum) {
  this->test_output(_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Sum}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateAvg) {
  this->test_output(_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Avg}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateCount) {
  this->test_output(_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Count}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, OneGroupbyAndNoAggregate) {
  this->test_output(_table_wrapper_1_1, {}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_0agg/result.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndNoAggregate) {
  this->test_output(_table_wrapper_1_1, {}, {ColumnID{0}, ColumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_0agg/result.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbyAndNoAggregate) {
  EXPECT_THROW(std::make_shared<Aggregate>(_table_wrapper_1_1_string, std::vector<AggregateDefinition>{},
                                           std::vector<ColumnID>{}),
               std::logic_error);
}

/**
 * Tests for NULL values
 */
TEST_F(OperatorsAggregateTest, CanCountStringColumnsWithNull) {
  this->test_output(_table_wrapper_1_1_string_null, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/count_str_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMaxWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMinWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, SingleAggregateSumWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, SingleAggregateAvgWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, SingleAggregateCountWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, OneGroupbyAndNoAggregateWithNull) {
  this->test_output(_table_wrapper_1_1_null, {}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_0agg/result_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, OneGroupbyCountStar) {
  this->test_output(_table_wrapper_1_1_null, {{CountStarID, AggregateFunction::Count}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_0agg/count_star.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyCountStar) {
  this->test_output(_table_wrapper_2_0_null, {{CountStarID, AggregateFunction::Count}}, {ColumnID{0}, ColumnID{2}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_0agg/count_star.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMaxWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMinWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateSumWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateAvgWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl", 1, false);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateCountWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count_null.tbl", 1, false);
}

/**
 * Tests for ReferenceColumns
 */

TEST_F(OperatorsAggregateTest, SingleAggregateMaxOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_1_1, ColumnID{0}, ScanType::LessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_2_2, ColumnID{0}, ScanType::LessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{ColumnID{2}, AggregateFunction::Min}, {ColumnID{3}, AggregateFunction::Avg}},
                    {ColumnID{0}, ColumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/min_avg_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbySumOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_2_1, ColumnID{0}, ScanType::LessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{ColumnID{2}, AggregateFunction::Sum}}, {ColumnID{0}, ColumnID{1}},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/sum_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_1_2, ColumnID{0}, ScanType::LessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{ColumnID{1}, AggregateFunction::Sum}, {ColumnID{2}, AggregateFunction::Avg}},
                    {ColumnID{0}}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_avg_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMinOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_1_1_dict, ColumnID{0}, ScanType::LessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, JoinThenAggregate) {
  auto join = std::make_shared<JoinHash>(_table_wrapper_3_1, _table_wrapper_3_2, JoinMode::Inner,
                                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::Equals);
  join->execute();

  this->test_output(join, {}, {ColumnID{0}, ColumnID{3}}, "src/test/tables/aggregateoperator/join_2gb_0agg/result.tbl",
                    1);
}

TEST_F(OperatorsAggregateTest, OuterJoinThenAggregate) {
  auto join =
      std::make_shared<JoinNestedLoop>(_table_wrapper_join_1, _table_wrapper_join_2, JoinMode::Outer,
                                       std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::LessThan);
  join->execute();

  this->test_output(join, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/outer_join.tbl", 1, false);
}

}  // namespace opossum
