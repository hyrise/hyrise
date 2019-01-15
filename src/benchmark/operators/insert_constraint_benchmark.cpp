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

auto reset_table(bool use_constraints, int values_to_insert, int num_rows = 0) {
  auto& manager = StorageManager::get();

  auto chunk_size = ChunkID(opossum::ChunkID{2000});

  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("column0", DataType::Int, true);
  column_definitions.emplace_back("column1", DataType::Int, false);

  // Create table on which the insert operators will work on
  manager.reset();
  manager.add_table("table", std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes));
  auto table = manager.get_table("table");
  if (use_constraints) {
    table->add_unique_constraint({ColumnID{0}});
    table->add_unique_constraint({ColumnID{1}});
  }

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

  // Create insert operators depending on values to insert operator
  std::vector<std::shared_ptr<Insert>> table_inserts;
  for (int row_to_insert = row_preinserted; row_to_insert < values_to_insert + num_rows; row_to_insert++) {
    auto table_temp = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);
    manager.add_table("table_temp" + std::to_string(row_to_insert), table_temp);
    table_temp->append({row_to_insert, row_to_insert * 2});
    auto gt = std::make_shared<GetTable>("table_temp" + std::to_string(row_to_insert));
    gt->execute();
    auto table_insert = std::make_shared<Insert>("table", gt);
    table_inserts.push_back(table_insert);
  }

  return table_inserts;
}

BENCHMARK_DEFINE_F(MicroBenchmarkBasicFixture, BM_InsertEmptyTableWithConstraint)(benchmark::State& state) {
  while (state.KeepRunning()) {
    state.PauseTiming();

    auto table_inserts = reset_table(state.range(0), 1000);

    state.ResumeTiming();

    for (const auto& table_insert : table_inserts) {
      auto table_context = TransactionManager::get().new_transaction_context();
      table_insert->set_transaction_context(table_context);
      table_insert->execute();
      table_context->commit();
    }
  }
}

static void InsertRanges(benchmark::internal::Benchmark* b) {
  for (uint32_t j = 1; j <= 5000; j *= 2) {
    b->Args({true, j});
    b->Args({false, j});
  }
}

BENCHMARK_REGISTER_F(MicroBenchmarkBasicFixture, BM_InsertEmptyTableWithConstraint)->Apply(InsertRanges);

BENCHMARK_DEFINE_F(MicroBenchmarkBasicFixture, BM_InsertFilledTableWithConstraint)(benchmark::State& state) {
  while (state.KeepRunning()) {
    state.PauseTiming();

    auto table_inserts = reset_table(state.range(0), 1, static_cast<int>(state.range(1)));

    state.ResumeTiming();

    for (const auto& table_insert : table_inserts) {
      auto table_context = TransactionManager::get().new_transaction_context();
      table_insert->set_transaction_context(table_context);
      table_insert->execute();
      table_context->commit();
    }
  }
}

static void PreInsertRanges(benchmark::internal::Benchmark* b) {
  for (uint32_t j = 1; j <= 5000; j *= 2) {
    b->Args({true, j});
    b->Args({false, j});
  }
}

BENCHMARK_REGISTER_F(MicroBenchmarkBasicFixture, BM_InsertFilledTableWithConstraint)->Apply(PreInsertRanges);

}  // namespace opossum
