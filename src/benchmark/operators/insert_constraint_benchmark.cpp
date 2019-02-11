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

auto create_table_and_operator(const int num_rows, const bool use_constraints, const bool use_compression) {
  auto& manager = StorageManager::get();

  auto chunk_size = ChunkID(opossum::ChunkID{1000});

  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("column0", DataType::Int, true);
  column_definitions.emplace_back("column1", DataType::Int, false);

  // Create table on which the insert operators will work on
  manager.reset();
  manager.add_table("table", std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes));
  auto table = manager.get_table("table");

  // Insert rows to table, if num_rows != 0
  int row_preinserted = 0;
  auto pre_insert_table_temp = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);
  manager.add_table("pre_insert_table_temp", pre_insert_table_temp);

  for (; row_preinserted < num_rows; row_preinserted++) {
    pre_insert_table_temp->append({row_preinserted, row_preinserted * 2});
  }

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

  auto table_temp = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);
  manager.add_table("table_temp", table_temp);
  table_temp->append({-1, -2});
  auto gt = std::make_shared<GetTable>("table_temp");
  gt->execute();
  auto table_insert = std::make_shared<Insert>("table", gt);

  return table_insert;
}

BENCHMARK_DEFINE_F(MicroBenchmarkBasicFixture, BM_InsertFilledTableWithConstraint)(benchmark::State& state) {
  while (state.KeepRunning()) {
    state.PauseTiming();

    auto table_insert = create_table_and_operator(static_cast<int>(state.range(1)), state.range(0), false);

    state.ResumeTiming();

    auto table_context = TransactionManager::get().new_transaction_context();
    table_insert->set_transaction_context(table_context);
    table_insert->execute();
    table_context->commit();
  }
}

static void InsertRangesFilledTable(benchmark::internal::Benchmark* b) {
  for (uint32_t j = 100; j <= 150000; j *= 2) {
    b->Args({true, j});
    b->Args({false, j});
  }
}

BENCHMARK_REGISTER_F(MicroBenchmarkBasicFixture, BM_InsertFilledTableWithConstraint)
    ->Apply(InsertRangesFilledTable)
    ->Iterations(100);

BENCHMARK_DEFINE_F(MicroBenchmarkBasicFixture, BM_InsertOnCompressedTable)(benchmark::State& state) {
  create_table_and_operator(static_cast<int>(state.range(1)), state.range(0), true);
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");

  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("column0", DataType::Int, true);
  column_definitions.emplace_back("column1", DataType::Int, false);
  auto chunk_size = ChunkID(opossum::ChunkID{2000});

  // Create insert operators depending on values to insert operator

  while (state.KeepRunning()) {
    state.PauseTiming();

    auto table_temp = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);
    const int row_count = static_cast<int>(table->row_count());
    manager.add_table("table_temp" + std::to_string(row_count), table_temp);

    table_temp->append({row_count, row_count * 2});
    auto gt = std::make_shared<GetTable>("table_temp" + std::to_string(row_count));
    gt->execute();

    auto table_insert = std::make_shared<Insert>("table", gt);
    auto table_context = TransactionManager::get().new_transaction_context();
    table_insert->set_transaction_context(table_context);

    state.ResumeTiming();

    table_insert->execute();
    table_context->commit();
  }
}

BENCHMARK_REGISTER_F(MicroBenchmarkBasicFixture, BM_InsertOnCompressedTable)
    ->Apply(InsertRangesFilledTable)
    ->Iterations(100);

}  // namespace opossum
