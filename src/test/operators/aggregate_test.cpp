#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_read_only_operator.hpp"
#include "operators/aggregate.hpp"
#include "operators/join_hash.hpp"
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

  void test_output(const std::shared_ptr<AbstractOperator> in, const std::vector<AggregateDefinition> &aggregates,
                   const std::vector<std::string> &groupby_columns, const std::string &file_name, size_t chunk_size) {
    // load expected results from file
    std::shared_ptr<Table> expected_result = load_table(file_name, chunk_size);
    EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

    // collect possible columns to scan before aggregate
    std::set<std::string> ref_columns;
    ref_columns.insert("");

    for (auto const &agg : aggregates) {
      if (agg.column_name != "*") {
        ref_columns.insert(agg.column_name);
      }
    }

    for (auto const &groupby : groupby_columns) {
      ref_columns.insert(groupby);
    }

    for (auto &ref : ref_columns) {
      // make one Aggregate w/o ReferenceColumn
      auto input = in;

      if (ref != "") {
        // also try a TableScan on every involved column
        auto column_id = in->get_output()->column_id_by_name(ref);
        if (in->get_output()->column_type(column_id) == "string") {
          input = std::make_shared<TableScan>(in, ref, ScanType::OpNotEquals, std::string("something"));
        } else {
          input = std::make_shared<TableScan>(in, ref, ScanType::OpGreaterThanEquals, 0);
        }

        input->execute();
      }

      // build and execute Aggregate
      auto aggregate = std::make_shared<Aggregate>(input, aggregates, groupby_columns);
      EXPECT_NE(aggregate, nullptr) << "Could not build Aggregate";
      aggregate->execute();
      EXPECT_TABLE_EQ(aggregate->get_output(), expected_result);
    }
  }

  std::shared_ptr<TableWrapper> _table_wrapper_1_1, _table_wrapper_1_1_null, _table_wrapper_1_2, _table_wrapper_2_1,
      _table_wrapper_2_2, _table_wrapper_2_0_null, _table_wrapper_1_1_string, _table_wrapper_1_1_string_null,
      _table_wrapper_1_1_dict, _table_wrapper_1_1_null_dict, _table_wrapper_3_1, _table_wrapper_3_2;
};

TEST_F(OperatorsAggregateTest, NumInputTables) {
  auto aggregate =
      std::make_shared<Aggregate>(_table_wrapper_1_1, std::vector<AggregateDefinition>{{"b", AggregateFunction::Max}},
                                  std::vector<std::string>{std::string("a")});
  aggregate->execute();

  EXPECT_EQ(aggregate->num_in_tables(), 1);
}

TEST_F(OperatorsAggregateTest, NumOutputTables) {
  auto aggregate =
      std::make_shared<Aggregate>(_table_wrapper_1_1, std::vector<AggregateDefinition>{{"b", AggregateFunction::Max}},
                                  std::vector<std::string>{std::string("a")});

  EXPECT_EQ(aggregate->num_out_tables(), 1);
}

TEST_F(OperatorsAggregateTest, OperatorName) {
  auto aggregate =
      std::make_shared<Aggregate>(_table_wrapper_1_1, std::vector<AggregateDefinition>{{"b", AggregateFunction::Max}},
                                  std::vector<std::string>{std::string("a")});

  EXPECT_EQ(aggregate->name(), "Aggregate");
}

TEST_F(OperatorsAggregateTest, CannotSumStringColumns) {
  auto aggregate = std::make_shared<Aggregate>(_table_wrapper_1_1_string,
                                               std::vector<AggregateDefinition>{{"a", AggregateFunction::Sum}},
                                               std::vector<std::string>{std::string("a")});

  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TEST_F(OperatorsAggregateTest, CannotAvgStringColumns) {
  auto aggregate = std::make_shared<Aggregate>(_table_wrapper_1_1_string,
                                               std::vector<AggregateDefinition>{{"a", AggregateFunction::Avg}},
                                               std::vector<std::string>{std::string("a")});

  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TEST_F(OperatorsAggregateTest, CanCountStringColumns) {
  this->test_output(_table_wrapper_1_1_string, {{"a", AggregateFunction::Count}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/count_str.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMax) {
  this->test_output(_table_wrapper_1_1, {{"b", AggregateFunction::Max}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMin) {
  this->test_output(_table_wrapper_1_1, {{"b", AggregateFunction::Min}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateSum) {
  this->test_output(_table_wrapper_1_1, {{"b", AggregateFunction::Sum}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateAvg) {
  this->test_output(_table_wrapper_1_1, {{"b", AggregateFunction::Avg}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateCount) {
  this->test_output(_table_wrapper_1_1, {{"b", AggregateFunction::Count}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateMax) {
  this->test_output(_table_wrapper_1_1_string, {{"b", AggregateFunction::Max}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateMin) {
  this->test_output(_table_wrapper_1_1_string, {{"b", AggregateFunction::Min}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateStringMax) {
  this->test_output(_table_wrapper_1_1_string, {{"a", AggregateFunction::Max}}, {},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/max_str.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateStringMin) {
  this->test_output(_table_wrapper_1_1_string, {{"a", AggregateFunction::Min}}, {},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/min_str.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateSum) {
  this->test_output(_table_wrapper_1_1_string, {{"b", AggregateFunction::Sum}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateAvg) {
  this->test_output(_table_wrapper_1_1_string, {{"b", AggregateFunction::Avg}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateCount) {
  this->test_output(_table_wrapper_1_1_string, {{"b", AggregateFunction::Count}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMax) {
  this->test_output(_table_wrapper_1_1_dict, {{"b", AggregateFunction::Max}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMin) {
  this->test_output(_table_wrapper_1_1_dict, {{"b", AggregateFunction::Min}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateSum) {
  this->test_output(_table_wrapper_1_1_dict, {{"b", AggregateFunction::Sum}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateAvg) {
  this->test_output(_table_wrapper_1_1_dict, {{"b", AggregateFunction::Avg}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateCount) {
  this->test_output(_table_wrapper_1_1_dict, {{"b", AggregateFunction::Count}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateAvgMax) {
  this->test_output(_table_wrapper_1_2, {{"b", AggregateFunction::Max}, {"c", AggregateFunction::Avg}},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/max_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateMinAvg) {
  this->test_output(_table_wrapper_1_2, {{"b", AggregateFunction::Min}, {"c", AggregateFunction::Avg}},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/min_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateMinMax) {
  this->test_output(_table_wrapper_1_2, {{"b", AggregateFunction::Min}, {"c", AggregateFunction::Max}},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/min_max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateAvgAvg) {
  this->test_output(_table_wrapper_1_2, {{"b", AggregateFunction::Avg}, {"c", AggregateFunction::Avg}},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/avg_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumAvg) {
  this->test_output(_table_wrapper_1_2, {{"b", AggregateFunction::Sum}, {"c", AggregateFunction::Avg}},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumAvgAlias) {
  this->test_output(_table_wrapper_1_2, {{"b", AggregateFunction::Sum, {"sum_b"}}, {"c", AggregateFunction::Avg}},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_avg_alias.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumSum) {
  this->test_output(_table_wrapper_1_2, {{"b", AggregateFunction::Sum}, {"c", AggregateFunction::Sum}},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumCount) {
  this->test_output(_table_wrapper_1_2, {{"b", AggregateFunction::Sum}, {"c", AggregateFunction::Count}},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyMax) {
  this->test_output(_table_wrapper_2_1, {{"c", AggregateFunction::Max}}, {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyMin) {
  this->test_output(_table_wrapper_2_1, {{"c", AggregateFunction::Min}}, {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbySum) {
  this->test_output(_table_wrapper_2_1, {{"c", AggregateFunction::Sum}}, {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAvg) {
  this->test_output(_table_wrapper_2_1, {{"c", AggregateFunction::Avg}}, {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyCount) {
  this->test_output(_table_wrapper_2_1, {{"c", AggregateFunction::Count}}, {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMaxAvg) {
  this->test_output(_table_wrapper_2_2, {{"c", AggregateFunction::Max}, {"d", AggregateFunction::Avg}},
                    {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvg) {
  this->test_output(_table_wrapper_2_2, {{"c", AggregateFunction::Min}, {"d", AggregateFunction::Avg}},
                    {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/min_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinMax) {
  this->test_output(_table_wrapper_2_2, {{"c", AggregateFunction::Min}, {"d", AggregateFunction::Max}},
                    {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/min_max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumAvg) {
  this->test_output(_table_wrapper_2_2, {{"c", AggregateFunction::Sum}, {"d", AggregateFunction::Avg}},
                    {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/sum_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumSum) {
  this->test_output(_table_wrapper_2_2, {{"c", AggregateFunction::Sum}, {"d", AggregateFunction::Sum}},
                    {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/sum_sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumCount) {
  this->test_output(_table_wrapper_2_2, {{"c", AggregateFunction::Sum}, {"d", AggregateFunction::Count}},
                    {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/sum_count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateMax) {
  this->test_output(_table_wrapper_1_1, {{"b", AggregateFunction::Max}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateMin) {
  this->test_output(_table_wrapper_1_1, {{"b", AggregateFunction::Min}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateSum) {
  this->test_output(_table_wrapper_1_1, {{"b", AggregateFunction::Sum}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateAvg) {
  this->test_output(_table_wrapper_1_1, {{"b", AggregateFunction::Avg}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbySingleAggregateCount) {
  this->test_output(_table_wrapper_1_1, {{"b", AggregateFunction::Count}}, {},
                    "src/test/tables/aggregateoperator/0gb_1agg/count.tbl", 1);
}

TEST_F(OperatorsAggregateTest, OneGroupbyAndNoAggregate) {
  this->test_output(_table_wrapper_1_1, {}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_0agg/result.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndNoAggregate) {
  this->test_output(_table_wrapper_1_1, {}, {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_0agg/result.tbl", 1);
}

TEST_F(OperatorsAggregateTest, NoGroupbyAndNoAggregate) {
  EXPECT_THROW(std::make_shared<Aggregate>(_table_wrapper_1_1_string, std::vector<AggregateDefinition>{},
                                           std::vector<std::string>{}),
               std::logic_error);
}

/**
 * Tests for NULL values
 */
TEST_F(OperatorsAggregateTest, CanCountStringColumnsWithNull) {
  this->test_output(_table_wrapper_1_1_string_null, {{"b", AggregateFunction::Count}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/count_str_null.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMaxWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{"b", AggregateFunction::Max}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMinWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{"b", AggregateFunction::Min}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateSumWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{"b", AggregateFunction::Sum}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateAvgWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{"b", AggregateFunction::Avg}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateCountWithNull) {
  this->test_output(_table_wrapper_1_1_null, {{"b", AggregateFunction::Count}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count_null.tbl", 1);
}

TEST_F(OperatorsAggregateTest, OneGroupbyAndNoAggregateWithNull) {
  this->test_output(_table_wrapper_1_1_null, {}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_0agg/result_null.tbl", 1);
}

TEST_F(OperatorsAggregateTest, OneGroupbyCountStar) {
  this->test_output(_table_wrapper_1_1_null, {{"*", AggregateFunction::Count}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_0agg/count_star.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyCountStar) {
  this->test_output(_table_wrapper_2_0_null, {{"*", AggregateFunction::Count}}, {std::string("a"), std::string("c")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_0agg/count_star.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMaxWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{"b", AggregateFunction::Max}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMinWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{"b", AggregateFunction::Min}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateSumWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{"b", AggregateFunction::Sum}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateAvgWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{"b", AggregateFunction::Avg}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateCountWithNull) {
  this->test_output(_table_wrapper_1_1_null_dict, {{"b", AggregateFunction::Count}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count_null.tbl", 1);
}

/**
 * Tests for ReferenceColumns
 */

TEST_F(OperatorsAggregateTest, SingleAggregateMaxOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_1_1, "a", ScanType::OpLessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{"b", AggregateFunction::Max}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_2_2, "a", ScanType::OpLessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{"c", AggregateFunction::Min}, {"d", AggregateFunction::Avg}},
                    {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/min_avg_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbySumOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_2_1, "a", ScanType::OpLessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{"c", AggregateFunction::Sum}}, {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/sum_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_1_2, "a", ScanType::OpLessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{"b", AggregateFunction::Sum}, {"c", AggregateFunction::Avg}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_avg_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMinOnRef) {
  auto filtered = std::make_shared<TableScan>(_table_wrapper_1_1_dict, "a", ScanType::OpLessThan, "100");
  filtered->execute();

  this->test_output(filtered, {{"b", AggregateFunction::Min}}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min_filtered.tbl", 1);
}

TEST_F(OperatorsAggregateTest, JoinThenAggregate) {
  auto join =
      std::make_shared<JoinHash>(_table_wrapper_3_1, _table_wrapper_3_2, std::pair<std::string, std::string>("a", "a"),
                                 ScanType::OpEquals, JoinMode::Inner, std::string("left."), std::string("right."));
  join->execute();

  this->test_output(join, {}, {std::string("left.a"), std::string("right.b")},
                    "src/test/tables/aggregateoperator/join_2gb_0agg/result.tbl", 1);
}

}  // namespace opossum
