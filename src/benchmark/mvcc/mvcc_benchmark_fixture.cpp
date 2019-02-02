#include "mvcc_benchmark_fixture.hpp"

#include <memory>
#include <vector>

#include "benchmark/benchmark.h"

#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "operators/get_table.hpp"
#include "operators/maintenance/create_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {
constexpr auto CHUNK_SIZE = opossum::ChunkID{100000};
}  // namespace

namespace opossum {

void MVCC_Benchmark_Fixture::_incrementAllValuesByOne() {
  // Prepare Update Operator
  auto transaction_context = TransactionManager::get().new_transaction_context();

  auto get_table = std::make_shared<GetTable>(_table_name);
  get_table->execute();
  auto validate_table = std::make_shared<Validate>(get_table);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();

  auto update_expressions = expression_vector(add_(_column_a, 1));
  auto updated_values_projection = std::make_shared<Projection>(validate_table, update_expressions);
  updated_values_projection->execute();

  // Apply Update, increment each value by 1
  auto update_table = std::make_shared<Update>(_table_name, validate_table, updated_values_projection);
  update_table->set_transaction_context(transaction_context);
  update_table->execute();

  transaction_context->commit();
}

void MVCC_Benchmark_Fixture::_invalidateRecords(int recordsCount) {
  // With each UPDATE, 10 records are updated resp. invalidated.
  int requiredUpdatesCount = recordsCount / 10;

  for (int i = 0; i < requiredUpdatesCount; i++) {
    _incrementAllValuesByOne();
  }
}

void MVCC_Benchmark_Fixture::SetUp(::benchmark::State& state) {
  _table_name = "mvcc_table";
  _column_a = pqp_column_(ColumnID{0}, DataType::Int, false, "a");

  // Create a table with dummy data
  auto intTable =
      load_table("src/benchmark/mvcc/mvcc_benchmark_table.tbl", CHUNK_SIZE);  // TODO(anyone) move to resources folder
  StorageManager::get().add_table(_table_name, intTable);

  // Invalidate rows
  int invalidationCount = static_cast<int>(state.range());
  _invalidateRecords(invalidationCount);
}

void MVCC_Benchmark_Fixture::TearDown(::benchmark::State&) { StorageManager::reset(); }

void MVCC_Benchmark_Fixture::_clear_cache() {
  //TODO(Julian)
}

}  // namespace opossum
