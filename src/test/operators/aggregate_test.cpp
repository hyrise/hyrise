#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_read_only_operator.hpp"
#include "../../lib/operators/aggregate.hpp"
#include "../../lib/operators/chunk_compression.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsAggregateTest : public BaseTest {
 protected:
  void SetUp() override {
    auto test_table = load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(test_table));
    _gt_1_1 = std::make_shared<GetTable>("table_a");
    _gt_1_1->execute();

    test_table = load_table("src/test/tables/aggregateoperator/groupby_int_1gb_2agg/input.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(test_table));
    _gt_1_2 = std::make_shared<GetTable>("table_b");
    _gt_1_2->execute();

    test_table = load_table("src/test/tables/aggregateoperator/groupby_int_2gb_1agg/input.tbl", 2);
    StorageManager::get().add_table("table_c", std::move(test_table));
    _gt_2_1 = std::make_shared<GetTable>("table_c");
    _gt_2_1->execute();

    test_table = load_table("src/test/tables/aggregateoperator/groupby_int_2gb_2agg/input.tbl", 2);
    StorageManager::get().add_table("table_d", std::move(test_table));
    _gt_2_2 = std::make_shared<GetTable>("table_d");
    _gt_2_2->execute();

    test_table = load_table("src/test/tables/aggregateoperator/groupby_string_1gb_1agg/input.tbl", 2);
    StorageManager::get().add_table("table_a_string", std::move(test_table));
    _gt_1_1_string = std::make_shared<GetTable>("table_a_string");
    _gt_1_1_string->execute();

    test_table = load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2);
    StorageManager::get().add_table("table_a_dict", std::move(test_table));

    auto compression = std::make_shared<ChunkCompression>("table_a_dict", std::vector<ChunkID>{0u, 1u});
    compression->execute();

    _gt_1_1_dict = std::make_shared<GetTable>("table_a_dict");
    _gt_1_1_dict->execute();
  }

  void test_output(const std::shared_ptr<AbstractOperator> in,
                   const std::vector<std::pair<std::string, AggregateFunction>> aggregates,
                   const std::vector<std::string> groupby_columns, const std::string &file_name, size_t chunk_size) {
    // load expected results from file
    std::shared_ptr<Table> expected_result = load_table(file_name, chunk_size);
    EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

    // collect possible columns to scan before aggregate
    std::set<std::string> ref_columns;
    ref_columns.insert("");

    for (auto const &agg : aggregates) {
      ref_columns.insert(agg.first);
    }

    for (auto const &groupby : groupby_columns) {
      ref_columns.insert(groupby);
    }

    for (auto &ref : ref_columns) {
      // make one Aggregate w/o ReferenceColumn
      auto input = in;

      if (ref != "") {
        // also try a TableScan on every involved column
        input = std::make_shared<TableScan>(in, ref, ">=", 0);
        input->execute();
      }

      // build and execute Aggregate
      auto aggregate = std::make_shared<Aggregate>(input, aggregates, groupby_columns);
      EXPECT_NE(aggregate, nullptr) << "Could not build Aggregate";
      aggregate->execute();
      EXPECT_TABLE_EQ(aggregate->get_output(), expected_result);
    }
  }

  std::shared_ptr<GetTable> _gt_1_1, _gt_1_2, _gt_2_1, _gt_2_2, _gt_1_1_string, _gt_1_1_dict;
};

TEST_F(OperatorsAggregateTest, NumInputTables) {
  auto aggregate = std::make_shared<Aggregate>(
      _gt_1_1, std::vector<std::pair<std::string, AggregateFunction>>{std::make_pair(std::string("b"), Max)},
      std::vector<std::string>{std::string("a")});
  aggregate->execute();

  EXPECT_EQ(aggregate->num_in_tables(), 1);
}

TEST_F(OperatorsAggregateTest, DISABLED_NumOutputTables) {
  auto aggregate = std::make_shared<Aggregate>(
      _gt_1_1, std::vector<std::pair<std::string, AggregateFunction>>{std::make_pair(std::string("b"), Max)},
      std::vector<std::string>{std::string("a")});

  EXPECT_EQ(aggregate->num_out_tables(), 1);
}

TEST_F(OperatorsAggregateTest, OperatorName) {
  auto aggregate = std::make_shared<Aggregate>(
      _gt_1_1, std::vector<std::pair<std::string, AggregateFunction>>{std::make_pair(std::string("b"), Max)},
      std::vector<std::string>{std::string("a")});

  EXPECT_EQ(aggregate->name(), "Aggregate");
}

TEST_F(OperatorsAggregateTest, CannotAggregateStringColumns) {
  auto aggregate = std::make_shared<Aggregate>(
      _gt_1_1_string, std::vector<std::pair<std::string, AggregateFunction>>{std::make_pair(std::string("a"), Min)},
      std::vector<std::string>{std::string("a")});

  EXPECT_THROW(aggregate->execute(), std::runtime_error);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMax) {
  this->test_output(_gt_1_1, {std::make_pair(std::string("b"), Max)}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateMin) {
  this->test_output(_gt_1_1, {std::make_pair(std::string("b"), Min)}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateSum) {
  this->test_output(_gt_1_1, {std::make_pair(std::string("b"), Sum)}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, SingleAggregateAvg) {
  this->test_output(_gt_1_1, {std::make_pair(std::string("b"), Avg)}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateMax) {
  this->test_output(_gt_1_1_string, {std::make_pair(std::string("b"), Max)}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateMin) {
  this->test_output(_gt_1_1_string, {std::make_pair(std::string("b"), Min)}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DISABLED_StringSingleAggregateSum) {
  this->test_output(_gt_1_1_string, {std::make_pair(std::string("b"), Sum)}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, StringSingleAggregateAvg) {
  this->test_output(_gt_1_1_string, {std::make_pair(std::string("b"), Avg)}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_string_1gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMax) {
  this->test_output(_gt_1_1_dict, {std::make_pair(std::string("b"), Max)}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateMin) {
  this->test_output(_gt_1_1_dict, {std::make_pair(std::string("b"), Min)}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateSum) {
  this->test_output(_gt_1_1_dict, {std::make_pair(std::string("b"), Sum)}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, DictionarySingleAggregateAvg) {
  this->test_output(_gt_1_1_dict, {std::make_pair(std::string("b"), Avg)}, {std::string("a")},
                    "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateAvgMax) {
  this->test_output(_gt_1_2, {std::make_pair(std::string("b"), Max), std::make_pair(std::string("c"), Avg)},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/max_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateMinAvg) {
  this->test_output(_gt_1_2, {std::make_pair(std::string("b"), Min), std::make_pair(std::string("c"), Avg)},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/min_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateMinMax) {
  this->test_output(_gt_1_2, {std::make_pair(std::string("b"), Min), std::make_pair(std::string("c"), Max)},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/min_max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateAvgAvg) {
  this->test_output(_gt_1_2, {std::make_pair(std::string("b"), Avg), std::make_pair(std::string("c"), Avg)},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/avg_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumAvg) {
  this->test_output(_gt_1_2, {std::make_pair(std::string("b"), Sum), std::make_pair(std::string("c"), Avg)},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoAggregateSumSum) {
  this->test_output(_gt_1_2, {std::make_pair(std::string("b"), Sum), std::make_pair(std::string("c"), Sum)},
                    {std::string("a")}, "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyMax) {
  this->test_output(_gt_2_1, {std::make_pair(std::string("c"), Max)}, {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyMin) {
  this->test_output(_gt_2_1, {std::make_pair(std::string("c"), Min)}, {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/min.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbySum) {
  this->test_output(_gt_2_1, {std::make_pair(std::string("c"), Sum)}, {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/sum.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAvg) {
  this->test_output(_gt_2_1, {std::make_pair(std::string("c"), Avg)}, {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_1agg/avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMaxAvg) {
  this->test_output(_gt_2_2, {std::make_pair(std::string("c"), Max), std::make_pair(std::string("d"), Avg)},
                    {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvg) {
  this->test_output(_gt_2_2, {std::make_pair(std::string("c"), Min), std::make_pair(std::string("d"), Avg)},
                    {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/min_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinMax) {
  this->test_output(_gt_2_2, {std::make_pair(std::string("c"), Min), std::make_pair(std::string("d"), Max)},
                    {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/min_max.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumAvg) {
  this->test_output(_gt_2_2, {std::make_pair(std::string("c"), Sum), std::make_pair(std::string("d"), Avg)},
                    {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/sum_avg.tbl", 1);
}

TEST_F(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumSum) {
  this->test_output(_gt_2_2, {std::make_pair(std::string("c"), Sum), std::make_pair(std::string("d"), Sum)},
                    {std::string("a"), std::string("b")},
                    "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/sum_sum.tbl", 1);
}

}  // namespace opossum
