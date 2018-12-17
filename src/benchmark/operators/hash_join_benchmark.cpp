#include <memory>

#include "benchmark/benchmark.h"
#include "operators/join_hash.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/storage_manager.hpp"
#include "table_generator.hpp"

namespace {
constexpr auto NUMBER_OF_CHUNKS = size_t{50};

// These numbers were arbitrarily chosen to form a representative group of JoinBenchmarks
// that run in a tolerable amount of time
constexpr auto TABLE_SIZE_SMALL = size_t{1'000};

void clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}
}  // namespace

namespace opossum {

std::shared_ptr<TableWrapper> generate_table(const size_t number_of_rows, const int smallFactor, const int bigFactor) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int);
  column_definitions.emplace_back("b", DataType::Int);

  std::shared_ptr<Table> table = std::make_shared<Table>(column_definitions, TableType::Data, number_of_rows);

  for (auto index = int{0}; index < static_cast<int>(number_of_rows); ++index) {
    table->append({index % smallFactor, index % bigFactor});
  }

  std::shared_ptr<TableWrapper> table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  return table_wrapper;
}

template <class C>
void bm_join_impl(benchmark::State& state, std::shared_ptr<TableWrapper> table_wrapper_left,
                  std::shared_ptr<TableWrapper> table_wrapper_right) {
  clear_cache();

  auto warm_up =
      std::make_shared<C>(table_wrapper_left, table_wrapper_right, JoinMode::Inner,
                          std::pair<ColumnID, ColumnID>{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto join =
        std::make_shared<C>(table_wrapper_left, table_wrapper_right, JoinMode::Inner,
                            std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    join->execute();
  }

  opossum::StorageManager::get().reset();
}

template <class C>
void BM_Hash_Join_SmallAndSmall(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  auto table_wrapper_left = generate_table(TABLE_SIZE_SMALL, 2, 8);
  auto table_wrapper_right = generate_table(TABLE_SIZE_SMALL, 4, 16);

  bm_join_impl<C>(state, table_wrapper_left, table_wrapper_right);
}

BENCHMARK_TEMPLATE(BM_Hash_Join_SmallAndSmall, JoinHash);
//BENCHMARK_TEMPLATE(BM_Join_SmallAndSmall, JoinHashMultiplePredicatesTest);

}  // namespace opossum
