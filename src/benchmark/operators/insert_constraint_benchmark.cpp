#include <memory>
#include <vector>

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "concurrency/transaction_manager.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

namespace opossum {

void prepare_prefilled_table(const int num_rows, const bool use_constraints, const bool use_compression) {
  auto& manager = StorageManager::get();

  auto chunk_size = ChunkID(opossum::ChunkID{1000});

  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("column0", DataType::Int, true);
  column_definitions.emplace_back("column1", DataType::Int, false);

  // Create table on which the insert operators will work on
  manager.reset();
  manager.add_table("table", std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes));
  auto table = manager.get_table("table");

  // Pre Insert rows to measure the impact of the constraint checking when adding a value afterwards
  int row_preinserted = 0;
  auto pre_insert_table_temp = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);
  manager.add_table("pre_insert_table_temp", pre_insert_table_temp);

  for (; row_preinserted < num_rows; row_preinserted++) {
    pre_insert_table_temp->append({row_preinserted, row_preinserted * 2});
  }

  // Insert the values to the actual table via another insert operator to generate the MVCC Data
  auto pre_insert_gt = std::make_shared<GetTable>("pre_insert_table_temp");
  pre_insert_gt->execute();
  auto pre_insert = std::make_shared<Insert>("table", pre_insert_gt);
  auto pre_insert_context = TransactionManager::get().new_transaction_context();
  pre_insert->set_transaction_context(pre_insert_context);
  pre_insert->execute();
  pre_insert_context->commit();

  if (use_constraints) {
    table->add_unique_constraint({ColumnID{0}});
    table->add_unique_constraint({ColumnID{1}});
  }

  if (use_compression) {
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});
  }
}

BENCHMARK_DEFINE_F(MicroBenchmarkBasicFixture, BM_InsertFilledTableWithConstraint)(benchmark::State& state) {
  auto& manager = StorageManager::get();
  // Pre define column structure since it can be
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("column0", DataType::Int, true);
  column_definitions.emplace_back("column1", DataType::Int, false);

  while (state.KeepRunning()) {
    // Pause Timing to set up test table
    state.PauseTiming();

    prepare_prefilled_table(static_cast<int>(state.range(1)), state.range(0), false);

    // Create the operator to be measured. By adding values below 0 the uniqueness is guaranteed
    auto table_temp = std::make_shared<Table>(column_definitions, TableType::Data, 1, UseMvcc::Yes);
    manager.add_table("table_temp", table_temp);
    table_temp->append({-1, -2});

    auto gt = std::make_shared<GetTable>("table_temp");
    gt->execute();
    auto insert_op = std::make_shared<Insert>("table", gt);

    auto table_context = TransactionManager::get().new_transaction_context();
    insert_op->set_transaction_context(table_context);

    state.ResumeTiming();

    // Actually execute the operator and trigger the constraint satiesfied check
    insert_op->execute();
    table_context->commit();
  }
}

static void insert_ranges_filled_tables(benchmark::internal::Benchmark* b) {
  for (uint32_t j = 100; j <= 150000; j *= 2) {
    // The bool determines if constraints are used
    // The int determines how many rows are pre-inserted
    b->Args({true, j});
    b->Args({false, j});
  }
}

// The iterations are limited to 100 since the table set up takes quiet long
BENCHMARK_REGISTER_F(MicroBenchmarkBasicFixture, BM_InsertFilledTableWithConstraint)
    ->Apply(insert_ranges_filled_tables)
    ->Iterations(100);

BENCHMARK_DEFINE_F(MicroBenchmarkBasicFixture, BM_InsertOnCompressedTable)(benchmark::State& state) {
  prepare_prefilled_table(static_cast<int>(state.range(1)), state.range(0), true);
  auto& manager = StorageManager::get();
  // Retrieve table to keep track of the row count
  auto table = manager.get_table("table");

  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("column0", DataType::Int, true);
  column_definitions.emplace_back("column1", DataType::Int, false);

  while (state.KeepRunning()) {
    // Pause Timing to set up test table
    state.PauseTiming();

    // Since we don't call prepare_prefilled_table between the iterations we have to delete the temp table manually
    if (manager.has_table("table_temp")) {
      manager.drop_table("table_temp");
    }

    // Create the operator to be measured.
    // Since the table is prefilled with positive values, we can add negative values without violating the constraint.
    auto table_temp = std::make_shared<Table>(column_definitions, TableType::Data, 1, UseMvcc::Yes);
    manager.add_table("table_temp", table_temp);
    const int row_count = static_cast<int>(table->row_count());
    table_temp->append({-1 * row_count, -2 * row_count});

    auto gt = std::make_shared<GetTable>("table_temp");
    gt->execute();
    auto insert_op = std::make_shared<Insert>("table", gt);

    auto table_context = TransactionManager::get().new_transaction_context();
    insert_op->set_transaction_context(table_context);

    state.ResumeTiming();

    // Actually execute the operator and trigger the constraint satiesfied check
    insert_op->execute();
    table_context->commit();
  }
}

// The iterations are limited to 100 since the table set up takes quiete long
BENCHMARK_REGISTER_F(MicroBenchmarkBasicFixture, BM_InsertOnCompressedTable)
    ->Apply(insert_ranges_filled_tables)
    ->Iterations(100);

}  // namespace opossum
