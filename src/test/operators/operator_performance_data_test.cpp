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
#include "magic_enum.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/index/group_key/group_key_index.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorPerformanceDataTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table = load_table("resources/test_data/tbl/int_int.tbl", 2);
    _table_wrapper = std::make_shared<TableWrapper>(_table);
    _table_wrapper->execute();
  }

  inline static std::shared_ptr<Table> _table;
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
  auto table_scan = std::make_shared<TableScan>(
      table_wrapper, greater_than_(get_column_expression(table_wrapper, ColumnID{0}), 1));
  table_scan->execute();

  auto& performance_data = table_scan->performance_data;
  EXPECT_GT(performance_data->walltime.count(), 0ul);

  EXPECT_TRUE(performance_data->input_row_count_left);
  EXPECT_EQ(3, *(performance_data->input_row_count_left));

  EXPECT_FALSE(performance_data->input_row_count_right);

  EXPECT_EQ(2, *(performance_data->output_row_count));
}

TEST_F(OperatorPerformanceDataTest, JoinHashStageRuntimes) {
  auto join = std::make_shared<JoinHash>(
      _table_wrapper, _table_wrapper, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
  join->execute();

  auto& perf = static_cast<const StagedOperatorPerformanceData&>(*join->performance_data);

  for (const auto stage : magic_enum::enum_values<JoinHash::OperatorStages>()) {
    EXPECT_TRUE(perf.get_stage_runtime(*magic_enum::enum_index(stage)).count() > 0);
  }
}

// Check that steps of IndexJoin (indexed chunks/unindexed chunks) are executed as expected.
TEST_F(OperatorPerformanceDataTest, JoinIndexStageRuntimes) {
  // This test modifies a table. Hence, we create a new one for this test only.
  auto table = load_table("resources/test_data/tbl/int_int.tbl", 2);
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  {
    auto join = std::make_shared<JoinIndex>(
        table_wrapper, table_wrapper, JoinMode::Inner,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
    auto& perf = static_cast<const JoinIndex::PerformanceData&>(*join->performance_data);

    EXPECT_TRUE(perf.get_stage_runtime(*magic_enum::enum_index(JoinIndex::OperatorStages::IndexJoining)).count() == 0);
    EXPECT_TRUE(perf.get_stage_runtime(*magic_enum::enum_index(JoinIndex::OperatorStages::NestedLoopJoining)).count() >
                0);
    EXPECT_TRUE(perf.get_stage_runtime(*magic_enum::enum_index(JoinIndex::OperatorStages::OutputWriting)).count() > 0);
    EXPECT_EQ(perf.chunks_scanned_with_index, 0);
    EXPECT_EQ(perf.chunks_scanned_without_index, 2);
  }

  // Add group-key index (required dictionary encoding) to table
  ChunkEncoder::encode_all_chunks(table);
  table->create_index<GroupKeyIndex>({ColumnID{0}});

  {
    auto join = std::make_shared<JoinIndex>(
        table_wrapper, table_wrapper, JoinMode::Inner,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
    auto& perf = static_cast<const JoinIndex::PerformanceData&>(*join->performance_data);

    EXPECT_TRUE(perf.get_stage_runtime(*magic_enum::enum_index(JoinIndex::OperatorStages::IndexJoining)).count() > 0);
    EXPECT_TRUE(perf.get_stage_runtime(*magic_enum::enum_index(JoinIndex::OperatorStages::NestedLoopJoining)).count() ==
                0);
    EXPECT_TRUE(perf.get_stage_runtime(*magic_enum::enum_index(JoinIndex::OperatorStages::OutputWriting)).count() > 0);
    EXPECT_EQ(perf.chunks_scanned_with_index, 2);
    EXPECT_EQ(perf.chunks_scanned_without_index, 0);
  }
  {
    // insert should create unencoded (unindexed) chunk
    table->append({17, 17});

    auto join = std::make_shared<JoinIndex>(
        table_wrapper, table_wrapper, JoinMode::Inner,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
    auto& perf = static_cast<const JoinIndex::PerformanceData&>(*join->performance_data);

    EXPECT_TRUE(perf.get_stage_runtime(*magic_enum::enum_index(JoinIndex::OperatorStages::IndexJoining)).count() > 0);
    EXPECT_TRUE(perf.get_stage_runtime(*magic_enum::enum_index(JoinIndex::OperatorStages::NestedLoopJoining)).count() >
                0);
    EXPECT_TRUE(perf.get_stage_runtime(*magic_enum::enum_index(JoinIndex::OperatorStages::OutputWriting)).count() > 0);
    EXPECT_EQ(perf.chunks_scanned_with_index, 2);
    EXPECT_EQ(perf.chunks_scanned_without_index, 1);
  }
}

TEST_F(OperatorPerformanceDataTest, AggregateHashStageRuntimes) {
  auto aggregate = std::make_shared<AggregateHash>(
      _table_wrapper, std::vector<std::shared_ptr<AggregateExpression>>{
      min_(pqp_column_(ColumnID{0}, _table->column_data_type(ColumnID{0}),
                       _table->column_is_nullable(ColumnID{0}),
                       _table->column_name(ColumnID{0})))},
      std::initializer_list<ColumnID>{ColumnID{1}});

  


  aggregate->execute();

  auto& staged_performance_data = static_cast<const StagedOperatorPerformanceData&>(*aggregate->performance_data);

  for (const auto stage : magic_enum::enum_values<AggregateHash::OperatorStages>()) {
    EXPECT_TRUE(staged_performance_data.get_stage_runtime(*magic_enum::enum_index(stage)).count() > 0);
  }
}

// TODO: include mrks tests from #2061

}  // namespace opossum
