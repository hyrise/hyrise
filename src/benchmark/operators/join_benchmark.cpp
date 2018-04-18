#include <memory>

#include "../table_generator.hpp"
#include "benchmark/benchmark.h"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

void clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}

std::shared_ptr<TableWrapper> generate_table(const size_t number_of_rows, const size_t number_of_chunks,
                                             bool numa_distribute_chunks = false) {
  auto table_generator = std::make_shared<TableGenerator>();

  ColumnDataDistribution config = ColumnDataDistribution::make_uniform_config(0.0, 10000);
  const auto chunk_size = static_cast<ChunkID>(number_of_rows / number_of_chunks);
  Assert(chunk_size > 0, "The chunk size is 0 or less, can not generate such a table");

  auto table = table_generator->generate_table(std::vector<ColumnDataDistribution>{config}, number_of_rows, chunk_size,
                                               EncodingType::Dictionary/*, numa_distribute_chunks*/);

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);

    std::vector<ColumnID> columns{1};
    for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
      columns[0] = column_id;
      chunk->create_index<AdaptiveRadixTreeIndex>(columns);
    }
  }

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  return table_wrapper;
}

template <class C>
void BM_Join_impl(benchmark::State& state, std::shared_ptr<TableWrapper> table_wrapper_left,
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
void BM_Join(benchmark::State& state) {
  auto table_wrapper_left = generate_table(100000, 50);
  auto table_wrapper_right = generate_table(100000, 50);

  BM_Join_impl<C>(state, table_wrapper_left, table_wrapper_right);
}

BENCHMARK_TEMPLATE(BM_Join, JoinNestedLoop);
BENCHMARK_TEMPLATE(BM_Join, JoinIndex);
BENCHMARK_TEMPLATE(BM_Join, JoinHash);
BENCHMARK_TEMPLATE(BM_Join, JoinSortMerge);

}  // namespace opossum
