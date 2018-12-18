#include <memory>

#include "column_generator.hpp"
#include "types.hpp"

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "expression/binary_predicate_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/storage_manager.hpp"

namespace {

void clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}

}

namespace opossum {

void execute_multi_predicate_join(const std::shared_ptr<const AbstractOperator>& left,
                                  const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                  const std::vector<JoinPredicate>& join_predicates) {
  // execute join for the first join predicate
  std::shared_ptr<AbstractOperator> latest_operator = std::make_shared<JoinHash>(
      left, right, mode, join_predicates[0].column_id_pair, join_predicates[0].predicateCondition);
  latest_operator->execute();

  // execute table scans for the following predicates (ColumnVsColumnTableScan)
  for (size_t index = 1; index < join_predicates.size(); ++index) {
    const auto left_column_expr =
        PQPColumnExpression::from_table(*left->get_output(), join_predicates[index].column_id_pair.first);
    const auto right_column_expr =
        PQPColumnExpression::from_table(*right->get_output(), join_predicates[index].column_id_pair.second);
    const auto predicate = std::make_shared<BinaryPredicateExpression>(join_predicates[index].predicateCondition,
                                                                       left_column_expr, right_column_expr);
    latest_operator = std::make_shared<TableScan>(latest_operator, predicate);
    latest_operator->execute();
  }
}

void bm_join_impl(benchmark::State& state, std::shared_ptr<TableWrapper> table_wrapper_left,
                  std::shared_ptr<TableWrapper> table_wrapper_right, const std::vector<JoinPredicate>& join_predicates) {
  clear_cache();

  // Warm up
  execute_multi_predicate_join(table_wrapper_left, table_wrapper_right, JoinMode::Inner, join_predicates);

  while (state.KeepRunning()) {
    execute_multi_predicate_join(table_wrapper_left, table_wrapper_right, JoinMode::Inner, join_predicates);
  }

  opossum::StorageManager::get().reset();
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_OneToFive)(benchmark::State& state) {  // NOLINT 1,000 x 1,000

  const size_t chunk_size = 10'000;
  const size_t row_count = 5;
  const size_t max_value = 5;

  ColumnGenerator gen;


  const auto allow_value = [](int value) { return value != 2; };
  const auto get_value_without_join_partner = [](double rnd_real) { return 2; };

  const auto join_pair = gen
      .generate_joinable_table_pair({1}, chunk_size, row_count, 5 * row_count, 0, max_value, allow_value,
                                    get_value_without_join_partner);

  const auto table_wrapper_left = std::make_shared<TableWrapper>(join_pair->first);
  const auto table_wrapper_right = std::make_shared<TableWrapper>(join_pair->second);

  std::vector<JoinPredicate> join_predicates;
  join_predicates.emplace_back(JoinPredicate{ColumnIDPair{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});

  bm_join_impl(state, table_wrapper_left, table_wrapper_right, join_predicates);
}

}  // namespace opossum
