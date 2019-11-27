#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/join_hash.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorPerformanceDataTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table_wrapper =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int.tbl", 10));
    _table_wrapper->execute();
  }

  inline static std::shared_ptr<TableWrapper> _table_wrapper;
};

TEST_F(OperatorPerformanceDataTest, ElementsAreSet) {
  const TableColumnDefinitions column_definitions{{"a", DataType::Int, false}};
  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3);
  table->append({1});
  table->append({2});
  table->append({3});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto table_scan = std::make_shared<TableScan>(table_wrapper, greater_than_(BaseTest::get_column_expression(table_wrapper, ColumnID{0}), 1));
  table_scan->execute();

  auto& performance_data = table_scan->performance_data();
  EXPECT_GT(performance_data.walltime.count(), 0ul);

  EXPECT_TRUE(performance_data.input_row_count_left);
  EXPECT_EQ(3, *(performance_data.input_row_count_left));

  EXPECT_FALSE(performance_data.input_row_count_right);

  EXPECT_EQ(2, performance_data.output_row_count);
}

TEST_F(OperatorPerformanceDataTest, JoinHashStageRuntimes) {
  auto join = std::make_shared<JoinHash>(
      _table_wrapper, _table_wrapper, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
  join->execute();

  auto& staged_performance_data = static_cast<const StagedOperatorPerformanceData&>(join->performance_data());

  EXPECT_TRUE(static_cast<size_t>(staged_performance_data.get_stage_runtime(static_cast<int>(JoinHash::OperatorStages::Materialization)).count()) > 0);
  EXPECT_TRUE(static_cast<size_t>(staged_performance_data.get_stage_runtime(static_cast<int>(JoinHash::OperatorStages::Clustering)).count()) > 0);
  EXPECT_TRUE(static_cast<size_t>(staged_performance_data.get_stage_runtime(static_cast<int>(JoinHash::OperatorStages::Building)).count()) > 0);
  EXPECT_TRUE(static_cast<size_t>(staged_performance_data.get_stage_runtime(static_cast<int>(JoinHash::OperatorStages::Probing)).count()) > 0);
  EXPECT_TRUE(static_cast<size_t>(staged_performance_data.get_stage_runtime(static_cast<int>(JoinHash::OperatorStages::OutputWriting)).count()) > 0);
}

TEST_F(OperatorPerformanceDataTest, AggregateHashStageRuntimes) {
  auto aggregate = std::make_shared<AggregateHash>(_table_wrapper, std::initializer_list<AggregateColumnDefinition>{{ColumnID{0}, AggregateFunction::Min}}, std::initializer_list<ColumnID>{ColumnID{1}});
  aggregate->execute();

  auto& staged_performance_data = static_cast<const StagedOperatorPerformanceData&>(aggregate->performance_data());

  EXPECT_TRUE(static_cast<size_t>(staged_performance_data.get_stage_runtime(static_cast<int>(AggregateHash::OperatorStages::GroupByColumns)).count()) > 0);
  EXPECT_TRUE(static_cast<size_t>(staged_performance_data.get_stage_runtime(static_cast<int>(AggregateHash::OperatorStages::AggregateColumns)).count()) > 0);
  EXPECT_TRUE(static_cast<size_t>(staged_performance_data.get_stage_runtime(static_cast<int>(AggregateHash::OperatorStages::OutputWriting)).count()) > 0);
  EXPECT_THROW(static_cast<size_t>(staged_performance_data.get_stage_runtime(17).count()), std::logic_error);
}

}  // namespace opossum
