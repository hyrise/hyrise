#include <sstream>
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

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

class OperatorPerformanceDataTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table = load_table("resources/test_data/tbl/int_int.tbl", ChunkOffset{2});
    _table_wrapper = std::make_shared<TableWrapper>(_table);
    _table_wrapper->never_clear_output();
    _table_wrapper->execute();
  }

  inline static std::shared_ptr<Table> _table;
  inline static std::shared_ptr<TableWrapper> _table_wrapper;
};

TEST_F(OperatorPerformanceDataTest, ElementsAreSet) {
  const TableColumnDefinitions column_definitions{{"a", DataType::Int, false}};
  auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{3});
  table->append({1});
  table->append({2});
  table->append({3});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto table_scan =
      std::make_shared<TableScan>(table_wrapper, greater_than_(get_column_expression(table_wrapper, ColumnID{0}), 1));
  table_scan->execute();

  EXPECT_TRUE(table_scan->executed());
  auto& performance_data = table_scan->performance_data;
  EXPECT_TRUE(performance_data->has_output);
  EXPECT_GT(performance_data->walltime.count(), 0ul);
  EXPECT_EQ(performance_data->output_row_count, 2ul);
}

// Check for correct counting of skipped chunks/segment (this is different to chunk pruning, which happens within the
// optimizer) due to early exists via the dictionary and counting of chunks/segments that are sorted and have thus been
// scanned with a sorted search (i.e., binary search).
TEST_F(OperatorPerformanceDataTest, TableScanPerformanceData) {
  const TableColumnDefinitions column_definitions{{"a", DataType::Int, false}};
  auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});
  table->append({1});
  table->append({2});
  table->append({2});
  table->append({2});
  table->append({2});
  table->append({3});
  table->append({3});
  table->append({3});

  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table);
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->never_clear_output();
  table_wrapper->execute();
  const auto column_a = get_column_expression(table_wrapper, ColumnID{0});

  {
    auto table_scan = std::make_shared<TableScan>(table_wrapper, equals_(column_a, 2));
    table_scan->execute();

    auto& performance_data = dynamic_cast<TableScan::PerformanceData&>(*table_scan->performance_data);
    EXPECT_GT(performance_data.walltime.count(), 0ul);
    EXPECT_EQ(performance_data.num_chunks_with_early_out, 1ul);
    EXPECT_EQ(performance_data.num_chunks_with_all_rows_matching, 1ul);
    EXPECT_EQ(performance_data.num_chunks_with_binary_search, 0ul);
  }

  {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, equals_(column_a, 1));
    table_scan->execute();

    const auto& performance_data = dynamic_cast<TableScan::PerformanceData&>(*table_scan->performance_data);
    EXPECT_GT(performance_data.walltime.count(), 0ul);
    EXPECT_EQ(performance_data.num_chunks_with_early_out, 3ul);
    EXPECT_EQ(performance_data.num_chunks_with_all_rows_matching, 0ul);
    EXPECT_EQ(performance_data.num_chunks_with_binary_search, 0ul);
  }

  // Check counters for sorted segment scanning (value scan)
  table->get_chunk(ChunkID{0})->set_individually_sorted_by(SortColumnDefinition{ColumnID{0}, SortMode::Ascending});
  table->get_chunk(ChunkID{1})->set_individually_sorted_by(SortColumnDefinition{ColumnID{0}, SortMode::Ascending});
  table->get_chunk(ChunkID{2})->set_individually_sorted_by(SortColumnDefinition{ColumnID{0}, SortMode::Ascending});
  {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, equals_(column_a, 2));
    table_scan->execute();

    const auto& performance_data = dynamic_cast<TableScan::PerformanceData&>(*table_scan->performance_data);
    EXPECT_GT(performance_data.walltime.count(), 0ul);
    EXPECT_EQ(performance_data.num_chunks_with_early_out, 1ul);
    EXPECT_EQ(performance_data.num_chunks_with_all_rows_matching, 1ul);
    EXPECT_EQ(performance_data.num_chunks_with_binary_search, 2ul);
  }

  // Between scan
  {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, between_inclusive_(column_a, 3, 4));
    table_scan->execute();

    const auto& performance_data = dynamic_cast<TableScan::PerformanceData&>(*table_scan->performance_data);
    EXPECT_GT(performance_data.walltime.count(), 0ul);
    EXPECT_EQ(performance_data.num_chunks_with_early_out, 2ul);
    EXPECT_EQ(performance_data.num_chunks_with_all_rows_matching, 1ul);
    EXPECT_EQ(performance_data.num_chunks_with_binary_search, 1ul);
  }

  // Test that nullable columns do not contribute all-rows-matching shortcuts
  const TableColumnDefinitions nullable_column_definition = {{"a", DataType::Int, true}};
  table = std::make_shared<Table>(nullable_column_definition, TableType::Data, ChunkOffset{2});
  table->append({1});
  table->append({2});
  table->append({2});
  table->append({2});
  table->append({2});
  table->append({3});
  table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(table);

  table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->never_clear_output();
  table_wrapper->execute();

  // ColumnVsValue scan
  {
    const auto table_scan =
        std::make_shared<TableScan>(table_wrapper, equals_(get_column_expression(table_wrapper, ColumnID{0}), 2));
    table_scan->execute();

    const auto& performance_data = dynamic_cast<TableScan::PerformanceData&>(*table_scan->performance_data);
    EXPECT_GT(performance_data.walltime.count(), 0ul);
    EXPECT_EQ(performance_data.num_chunks_with_early_out, 0ul);
    EXPECT_EQ(performance_data.num_chunks_with_all_rows_matching, 0ul);
    EXPECT_EQ(performance_data.num_chunks_with_binary_search, 0ul);
  }

  // Between scan on nullable columns
  {
    const auto table_scan = std::make_shared<TableScan>(
        table_wrapper, between_inclusive_(get_column_expression(table_wrapper, ColumnID{0}), 1, 4));
    table_scan->execute();

    const auto& performance_data = dynamic_cast<TableScan::PerformanceData&>(*table_scan->performance_data);
    EXPECT_GT(performance_data.walltime.count(), 0ul);
    EXPECT_EQ(performance_data.num_chunks_with_early_out, 0ul);
    EXPECT_EQ(performance_data.num_chunks_with_all_rows_matching, 0ul);
    EXPECT_EQ(performance_data.num_chunks_with_binary_search, 0ul);
  }
}

TEST_F(OperatorPerformanceDataTest, JoinHashStepRuntimes) {
  const auto join = std::make_shared<JoinHash>(
      _table_wrapper, _table_wrapper, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
  join->execute();

  const auto& perf = dynamic_cast<const OperatorPerformanceData<JoinHash::OperatorSteps>&>(*join->performance_data);

  for (const auto step : magic_enum::enum_values<JoinHash::OperatorSteps>()) {
    if (step == JoinHash::OperatorSteps::Clustering) {
      // Clustering step (i.e., radix partitioning) is not executed for small joins.
      EXPECT_EQ(perf.get_step_runtime(step).count(), 0ul);
      continue;
    }
    EXPECT_GT(perf.get_step_runtime(step).count(), 0ul);
  }
}

TEST_F(OperatorPerformanceDataTest, JoinHashBloomFilterReductions) {
  const auto table_a = load_table("resources/test_data/tbl/int_int2.tbl", ChunkOffset{2});
  const auto table_wrapper_a = std::make_shared<TableWrapper>(table_a);
  table_wrapper_a->never_clear_output();
  table_wrapper_a->execute();

  const auto table_b =
      load_table("resources/test_data/tbl/int_int_shuffled.tbl", ChunkOffset{2});  // larger than int_int.tbl
  const auto table_wrapper_b = std::make_shared<TableWrapper>(table_b);
  table_wrapper_b->never_clear_output();
  table_wrapper_b->execute();

  // Check that std's hash for integer values is still the identity funtion. This assumption might break in the future.
  // In this case, some of the following tests might break as well.
  ASSERT_EQ(std::hash<uint32_t>{}(17), 17);

  // Inner join case: We check that the number of stored positions (required for all join types except semi/anti).
  // Depending on the Bloom filter implementation (which is currently depending on the implementation of std::hash),
  // we might store values and positions in the hash map that do not have a matching partner.
  const auto inner_join = std::make_shared<JoinHash>(
      table_wrapper_a, table_wrapper_b, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
  inner_join->execute();

  const auto& inner_perf = dynamic_cast<JoinHash::PerformanceData&>(*inner_join->performance_data);
  EXPECT_EQ(inner_perf.build_side_materialized_value_count, 4ul);  // matching values 2,6,2
  EXPECT_EQ(inner_perf.probe_side_materialized_value_count, 4ul);  // matching values 2,6,2
  EXPECT_EQ(inner_perf.hash_tables_distinct_value_count, 2ul);     // values 2,6
  EXPECT_EQ(inner_perf.hash_tables_position_count, 3ul);           // positions 1,2,3
  EXPECT_TRUE(inner_perf.left_input_is_build_side);

  // Semi join case: We check that no positions are stored (see explanation for "AllPositions" mode in hash map).
  // Further, we force the larger input to be the build side. As we first materialize the smaller side (i.e., the probe
  // side in this case) and create the initial bloom filter with that, there will be no reduction due to bloom
  // filtering on the probe side.
  const auto semi_join = std::make_shared<JoinHash>(
      table_wrapper_a, table_wrapper_b, JoinMode::Semi,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
  semi_join->execute();

  const auto& semi_perf = dynamic_cast<JoinHash::PerformanceData&>(*semi_join->performance_data);
  EXPECT_EQ(semi_perf.build_side_materialized_value_count, 4ul);
  EXPECT_EQ(semi_perf.probe_side_materialized_value_count, table_a->row_count());
  EXPECT_EQ(semi_perf.hash_tables_distinct_value_count, 2ul);
  EXPECT_FALSE(semi_perf.hash_tables_position_count);
  EXPECT_FALSE(semi_perf.left_input_is_build_side);
}

// Check that steps of IndexJoin (indexed chunks/unindexed chunks) are executed as expected.
TEST_F(OperatorPerformanceDataTest, JoinIndexStepRuntimes) {
  // This test modifies a table. Hence, we create a new one for this test only.
  auto table = load_table("resources/test_data/tbl/int_int.tbl", ChunkOffset{2});
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->never_clear_output();
  table_wrapper->execute();

  {
    auto join = std::make_shared<JoinIndex>(
        table_wrapper, table_wrapper, JoinMode::Inner,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
    auto& perf = dynamic_cast<const JoinIndex::PerformanceData&>(*join->performance_data);

    EXPECT_EQ(perf.get_step_runtime(JoinIndex::OperatorSteps::IndexJoining).count(), 0);
    EXPECT_GT(perf.get_step_runtime(JoinIndex::OperatorSteps::NestedLoopJoining).count(), 0);
    EXPECT_GT(perf.get_step_runtime(JoinIndex::OperatorSteps::OutputWriting).count(), 0);
    EXPECT_EQ(perf.chunks_scanned_with_index, 0);
    EXPECT_EQ(perf.chunks_scanned_without_index, 2);
    EXPECT_EQ(perf.right_input_is_index_side, true);
  }

  // Add group-key index (required dictionary encoding) to table
  ChunkEncoder::encode_all_chunks(table);
  table->create_index<GroupKeyIndex>({ColumnID{0}});

  {
    auto join = std::make_shared<JoinIndex>(
        table_wrapper, table_wrapper, JoinMode::Inner,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
    auto& perf = dynamic_cast<const JoinIndex::PerformanceData&>(*join->performance_data);

    EXPECT_GT(perf.get_step_runtime(JoinIndex::OperatorSteps::IndexJoining).count(), 0);
    EXPECT_EQ(perf.get_step_runtime(JoinIndex::OperatorSteps::NestedLoopJoining).count(), 0);
    EXPECT_GT(perf.get_step_runtime(JoinIndex::OperatorSteps::OutputWriting).count(), 0);
    EXPECT_EQ(perf.chunks_scanned_with_index, 2);
    EXPECT_EQ(perf.chunks_scanned_without_index, 0);
    EXPECT_EQ(perf.right_input_is_index_side, true);
  }
  {
    auto join = std::make_shared<JoinIndex>(
        table_wrapper, table_wrapper, JoinMode::Inner,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals},
        std::vector<OperatorJoinPredicate>{}, IndexSide::Left);
    join->execute();
    auto& perf = dynamic_cast<const JoinIndex::PerformanceData&>(*join->performance_data);

    EXPECT_GT(perf.get_step_runtime(JoinIndex::OperatorSteps::IndexJoining).count(), 0);
    EXPECT_EQ(perf.get_step_runtime(JoinIndex::OperatorSteps::NestedLoopJoining).count(), 0);
    EXPECT_GT(perf.get_step_runtime(JoinIndex::OperatorSteps::OutputWriting).count(), 0);
    EXPECT_EQ(perf.chunks_scanned_with_index, 2);
    EXPECT_EQ(perf.chunks_scanned_without_index, 0);
    EXPECT_EQ(perf.right_input_is_index_side, false);
  }
  {
    // insert should create unencoded (unindexed) chunk
    table->append({17, 17});

    auto join = std::make_shared<JoinIndex>(
        table_wrapper, table_wrapper, JoinMode::Inner,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
    auto& perf = dynamic_cast<const JoinIndex::PerformanceData&>(*join->performance_data);

    EXPECT_GT(perf.get_step_runtime(JoinIndex::OperatorSteps::IndexJoining).count(), 0);
    EXPECT_GT(perf.get_step_runtime(JoinIndex::OperatorSteps::NestedLoopJoining).count(), 0);
    EXPECT_GT(perf.get_step_runtime(JoinIndex::OperatorSteps::OutputWriting).count(), 0);
    EXPECT_EQ(perf.chunks_scanned_with_index, 2);
    EXPECT_EQ(perf.chunks_scanned_without_index, 1);
    EXPECT_EQ(perf.right_input_is_index_side, true);
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

  auto& performance_data =
      dynamic_cast<const OperatorPerformanceData<AggregateHash::OperatorSteps>&>(*aggregate->performance_data);

  for (const auto step : magic_enum::enum_values<AggregateHash::OperatorSteps>()) {
    EXPECT_GT(performance_data.get_step_runtime(step).count(), 0);
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
    EXPECT_TRUE(delete_op->executed());
    const auto& performance_data = delete_op->performance_data;
    EXPECT_GT(performance_data->walltime.count(), 0);
    EXPECT_FALSE(performance_data->has_output);
  }

  {
    EXPECT_TRUE(table_scan_1->executed());
    const auto& performance_data = table_scan_1->performance_data;
    EXPECT_GT(performance_data->walltime.count(), 0);
    EXPECT_TRUE(performance_data->has_output);
    EXPECT_EQ(performance_data->output_row_count, 1);
    EXPECT_EQ(performance_data->output_chunk_count, 1);
  }

  {
    EXPECT_TRUE(table_scan_2->executed());
    const auto& performance_data = table_scan_2->performance_data;
    EXPECT_GT(performance_data->walltime.count(), 0);
    EXPECT_TRUE(performance_data->has_output);
    EXPECT_EQ(performance_data->output_row_count, 0);
    EXPECT_EQ(performance_data->output_chunk_count, 0);
  }

  // Committing to avoid error "Has registered operators but has neither been committed nor rolled back."
  transaction_context->commit();
}

TEST_F(OperatorPerformanceDataTest, JoinHashPerformanceToOutputStream) {
  OperatorPerformanceData<JoinHash::OperatorSteps> performance_data;
  performance_data.set_step_runtime(JoinHash::OperatorSteps::BuildSideMaterializing, std::chrono::nanoseconds{17});
  performance_data.set_step_runtime(JoinHash::OperatorSteps::Probing, std::chrono::nanoseconds{17});
  performance_data.has_output = true;
  performance_data.output_row_count = 1u;
  performance_data.output_chunk_count = 1u;

  if constexpr (HYRISE_DEBUG) {
    performance_data.walltime = std::chrono::nanoseconds{2u};
    std::stringstream stringstream_throw;
    // output_to_stream() throws when cumulative step runtimes are larger than operator runtime.
    EXPECT_THROW(performance_data.output_to_stream(stringstream_throw, DescriptionMode::SingleLine), std::logic_error);
  }

  performance_data.walltime = std::chrono::nanoseconds{35u};
  std::stringstream stringstream;
  stringstream << performance_data;
  EXPECT_TRUE(
      stringstream.str().starts_with("Output: 1 row in 1 chunk, 35 ns. Operator step runtimes: BuildSideMaterializing"
                                     " 17 ns, ProbeSideMaterializing 0 ns, Clustering 0 ns, Building 0 ns, Probing"
                                     " 17 ns, OutputWriting 0 ns."));
}

TEST_F(OperatorPerformanceDataTest, OutputToStream) {
  auto performance_data = OperatorPerformanceData<AbstractOperatorPerformanceData::NoSteps>{};
  {
    std::stringstream stream;
    stream << performance_data;
    EXPECT_EQ(stream.str(), "No output.");
  }
  {
    std::stringstream stream;
    performance_data.has_output = true;
    performance_data.output_row_count = 2u;
    performance_data.output_chunk_count = 1u;
    performance_data.walltime = std::chrono::nanoseconds{999u};
    stream << performance_data;
    EXPECT_EQ(stream.str(), "Output: 2 rows in 1 chunk, 999 ns.");

    // Switch chunk/rows to test for plural.
    performance_data.output_row_count = 1u;
    performance_data.output_chunk_count = 2u;
    stream.str("");
    stream << performance_data;
    EXPECT_EQ(stream.str(), "Output: 1 row in 2 chunks, 999 ns.");
  }
}

}  // namespace hyrise
