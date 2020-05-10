#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <magic_enum.hpp>

#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
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

  EXPECT_EQ(2, performance_data->output_row_count);
}

// Check for correct counting of skipped chunks/segment (this is different to chunk pruning, which happens within the
// optimizer) due to early exists via the dictionary and counting of chunks/segments that are sorted and have thus been
// scanned with a sorted search (i.e., binary search).
TEST_F(OperatorPerformanceDataTest, TableScanPerformanceData) {
  const TableColumnDefinitions column_definitions{{"a", DataType::Int, false}};
  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
  table->append({1});
  table->append({2});
  table->append({2});
  table->append({3});

  // Finalize last chunk
  table->get_chunk(ChunkID{1})->finalize();

  ChunkEncoder::encode_all_chunks(table);

  {
    auto table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->execute();

    auto table_scan =
        std::make_shared<TableScan>(table_wrapper, equals_(get_column_expression(table_wrapper, ColumnID{0}), 2));
    table_scan->execute();

    auto& scan_performance_data = static_cast<TableScan::TableScanPerformanceData&>(*table_scan->performance_data);
    EXPECT_GT(scan_performance_data.walltime.count(), 0ul);
    EXPECT_EQ(0, scan_performance_data.chunk_scans_skipped);
    EXPECT_EQ(0, scan_performance_data.chunk_scans_sorted);
  }

  {
    auto table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->execute();

    auto table_scan =
        std::make_shared<TableScan>(table_wrapper, equals_(get_column_expression(table_wrapper, ColumnID{0}), 1));
    table_scan->execute();

    auto& scan_performance_data = static_cast<TableScan::TableScanPerformanceData&>(*table_scan->performance_data);
    EXPECT_GT(scan_performance_data.walltime.count(), 0ul);
    EXPECT_EQ(1, scan_performance_data.chunk_scans_skipped);
    EXPECT_EQ(0, scan_performance_data.chunk_scans_sorted);
  }

  // Check counters for sorted segment scanning (value scan)
  table->get_chunk(ChunkID{0})->set_ordered_by({ColumnID{0}, OrderByMode::Ascending});
  table->get_chunk(ChunkID{1})->set_ordered_by({ColumnID{0}, OrderByMode::Ascending});
  {
    auto table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->execute();

    auto table_scan =
        std::make_shared<TableScan>(table_wrapper, equals_(get_column_expression(table_wrapper, ColumnID{0}), 2));
    table_scan->execute();

    auto& scan_performance_data = static_cast<TableScan::TableScanPerformanceData&>(*table_scan->performance_data);
    EXPECT_GT(scan_performance_data.walltime.count(), 0ul);
    EXPECT_EQ(0, scan_performance_data.chunk_scans_skipped);
    EXPECT_EQ(2, scan_performance_data.chunk_scans_sorted);
  }

  // Between scan
  {
    auto table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->execute();

    auto table_scan = std::make_shared<TableScan>(
        table_wrapper, between_inclusive_(get_column_expression(table_wrapper, ColumnID{0}), 1, 2));
    table_scan->execute();

    auto& scan_performance_data = static_cast<TableScan::TableScanPerformanceData&>(*table_scan->performance_data);
    EXPECT_GT(scan_performance_data.walltime.count(), 0ul);
    EXPECT_EQ(0, scan_performance_data.chunk_scans_skipped);
    EXPECT_EQ(2, scan_performance_data.chunk_scans_sorted);
  }
}

TEST_F(OperatorPerformanceDataTest, JoinHashStepRuntimes) {
  auto join = std::make_shared<JoinHash>(
      _table_wrapper, _table_wrapper, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
  join->execute();

  auto& perf = static_cast<const StepOperatorPerformanceData&>(*join->performance_data);

  for (const auto step : magic_enum::enum_values<JoinHash::OperatorSteps>()) {
    EXPECT_GT(perf.get_step_runtime(static_cast<size_t>(step)).count(), 0);
  }
}

// Check that steps of IndexJoin (indexed chunks/unindexed chunks) are executed as expected.
TEST_F(OperatorPerformanceDataTest, JoinIndexStepRuntimes) {
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

    EXPECT_EQ(perf.get_step_runtime(static_cast<size_t>(JoinIndex::OperatorSteps::IndexJoining)).count(), 0);
    EXPECT_GT(perf.get_step_runtime(static_cast<size_t>(JoinIndex::OperatorSteps::NestedLoopJoining)).count(), 0);
    EXPECT_GT(perf.get_step_runtime(static_cast<size_t>(JoinIndex::OperatorSteps::OutputWriting)).count(), 0);
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

    EXPECT_GT(perf.get_step_runtime(static_cast<size_t>(JoinIndex::OperatorSteps::IndexJoining)).count(), 0);
    EXPECT_EQ(perf.get_step_runtime(static_cast<size_t>(JoinIndex::OperatorSteps::NestedLoopJoining)).count(), 0);
    EXPECT_GT(perf.get_step_runtime(static_cast<size_t>(JoinIndex::OperatorSteps::OutputWriting)).count(), 0);
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

    EXPECT_GT(perf.get_step_runtime(static_cast<size_t>(JoinIndex::OperatorSteps::IndexJoining)).count(), 0);
    EXPECT_GT(perf.get_step_runtime(static_cast<size_t>(JoinIndex::OperatorSteps::NestedLoopJoining)).count(), 0);
    EXPECT_GT(perf.get_step_runtime(static_cast<size_t>(JoinIndex::OperatorSteps::OutputWriting)).count(), 0);
    EXPECT_EQ(perf.chunks_scanned_with_index, 2);
    EXPECT_EQ(perf.chunks_scanned_without_index, 1);
  }
}

TEST_F(OperatorPerformanceDataTest, AggregateHashStepRuntimes) {
  auto aggregate = std::make_shared<AggregateHash>(
      _table_wrapper,
      std::vector<std::shared_ptr<AggregateExpression>>{
          min_(pqp_column_(ColumnID{0}, _table->column_data_type(ColumnID{0}), _table->column_is_nullable(ColumnID{0}),
                           _table->column_name(ColumnID{0})))},
      std::initializer_list<ColumnID>{ColumnID{1}});

  aggregate->execute();

  auto& step_performance_data = static_cast<const StepOperatorPerformanceData&>(*aggregate->performance_data);

  for (const auto step : magic_enum::enum_values<AggregateHash::OperatorSteps>()) {
    EXPECT_GT(step_performance_data.get_step_runtime(static_cast<size_t>(step)).count(), 0);
  }
}

TEST_F(OperatorPerformanceDataTest, OperatorPerformanceDataHasOutputMarkerSet) {
  const auto table_name = "table_a";
  const auto table = load_table("resources/test_data/tbl/int_float.tbl");

  // Delete Operator works with the Storage Manager, so the test table must also be known to the StorageManager
  Hyrise::get().storage_manager.add_table(table_name, table);

  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);

  auto gt = std::make_shared<GetTable>(table_name);
  gt->execute();

  auto table_scan_1 = create_table_scan(gt, ColumnID{0}, PredicateCondition::Equals, "123");
  auto table_scan_2 = create_table_scan(gt, ColumnID{0}, PredicateCondition::LessThan, "0");

  table_scan_1->execute();
  table_scan_2->execute();

  auto delete_op = std::make_shared<Delete>(table_scan_1);
  delete_op->set_transaction_context(transaction_context);

  delete_op->execute();
  EXPECT_FALSE(delete_op->execute_failed());

  {
    const auto& performance_data = delete_op->performance_data;
    EXPECT_TRUE(performance_data->executed);
    EXPECT_GT(performance_data->walltime.count(), 0);
    EXPECT_FALSE(performance_data->has_output);
  }

  {
    const auto& performance_data = table_scan_1->performance_data;
    EXPECT_TRUE(performance_data->executed);
    EXPECT_GT(performance_data->walltime.count(), 0);
    EXPECT_TRUE(performance_data->has_output);
    EXPECT_EQ(performance_data->output_row_count, 1);
    EXPECT_EQ(performance_data->output_chunk_count, 1);
  }

  {
    const auto& performance_data = table_scan_2->performance_data;
    EXPECT_TRUE(performance_data->executed);
    EXPECT_GT(performance_data->walltime.count(), 0);
    EXPECT_TRUE(performance_data->has_output);
    EXPECT_EQ(performance_data->output_row_count, 0);
    EXPECT_EQ(performance_data->output_chunk_count, 0);
  }

  // Committing to avoid error "Has registered operators but has neither been committed nor rolled back."
  transaction_context->commit();
}

// Ensure that only non-zero runtimes are listed but at the same time assume that steps might be skipped and thus zero
// runtimes can occur in between non-zero runtimes.
TEST_F(OperatorPerformanceDataTest, StepOperatorPerformanceDataToOutputStream) {
  StepOperatorPerformanceData performance_data;
  performance_data.step_runtimes[0] = std::chrono::nanoseconds{17};
  performance_data.step_runtimes[1] = std::chrono::nanoseconds{17};
  performance_data.step_runtimes[2] = std::chrono::nanoseconds{0};
  performance_data.step_runtimes[3] = std::chrono::nanoseconds{17};

  std::stringstream stringstream;
  stringstream << performance_data << std::endl;
  std::cout << stringstream.str() << std::endl;
  EXPECT_TRUE(stringstream.str().starts_with("Step runtimes: 17 ns, 17 ns, 0 ns, 17 ns."));
}

TEST_F(OperatorPerformanceDataTest, OutputToStream) {
  auto performance_data = OperatorPerformanceData{};
  {
    std::stringstream stream;
    stream << performance_data;
    EXPECT_EQ(stream.str(), "not executed");
  }
  {
    std::stringstream stream;
    performance_data.executed = true;
    stream << performance_data;
    EXPECT_EQ(stream.str(), "executed, but no output");
  }
  {
    std::stringstream stream;
    performance_data.has_output = true;
    performance_data.output_row_count = 2u;
    performance_data.output_chunk_count = 1u;
    performance_data.walltime = std::chrono::nanoseconds{999u};
    stream << performance_data;
    EXPECT_EQ(stream.str(), "2 row(s) in 1 chunk(s), 999 ns");
  }
}

}  // namespace opossum
