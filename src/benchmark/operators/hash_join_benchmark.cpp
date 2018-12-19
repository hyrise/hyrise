#include <memory>

#include "column_generator.hpp"
#include "types.hpp"

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "expression/binary_predicate_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/join_hash.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"

namespace {

const size_t CHUNK_SIZE = 10'000;
const size_t SCALE_FACTOR = 1'000'000;

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

void execute_multi_predicate_join(const std::shared_ptr<const AbstractOperator>& left,
                                  const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                  const std::vector<JoinPredicate>& join_predicates) {
  // execute join for the first join predicate
  std::shared_ptr<AbstractOperator> latest_operator = std::make_shared<JoinHash>(
      left, right, mode, join_predicates[0].column_id_pair, join_predicates[0].predicateCondition);
  latest_operator->execute();

  // std::cout << "Row count after first join: " << latest_operator->get_output()->row_count() << std::endl;
  // Print::print(left);
  // Print::print(right);
  // Print::print(latest_operator);

  // execute table scans for the following predicates (ColumnVsColumnTableScan)
  for (size_t index = 1; index < join_predicates.size(); ++index) {
    const auto left_column_expr =
        PQPColumnExpression::from_table(*latest_operator->get_output(), join_predicates[index].column_id_pair.first);
    const auto right_column_expr = PQPColumnExpression::from_table(
        *latest_operator->get_output(),
        static_cast<ColumnID>(left->get_output()->column_count() + join_predicates[index].column_id_pair.second));
    const auto predicate = std::make_shared<BinaryPredicateExpression>(join_predicates[index].predicateCondition,
                                                                       left_column_expr, right_column_expr);
    latest_operator = std::make_shared<TableScan>(latest_operator, predicate);
    latest_operator->execute();
    // Print::print(latest_operator);
    // std::cout << "Row count after " << index + 1 << " predicate: "
    //           << latest_operator->get_output()->row_count() << std::endl;
  }
}

void bm_join_impl(benchmark::State& state, std::shared_ptr<TableWrapper> table_wrapper_left,
                  std::shared_ptr<TableWrapper> table_wrapper_right,
                  const std::vector<JoinPredicate>& join_predicates) {
  clear_cache();

  // Warm up
  execute_multi_predicate_join(table_wrapper_left, table_wrapper_right, JoinMode::Inner, join_predicates);

  while (state.KeepRunning()) {
    execute_multi_predicate_join(table_wrapper_left, table_wrapper_right, JoinMode::Inner, join_predicates);
  }

  opossum::StorageManager::get().reset();
}

void execute_multi_predicate_join(benchmark::State& state, size_t chunk_size, size_t fact_table_size,
                                  size_t fact_factor, double probing_factor) {
  ColumnGenerator gen;

  const auto join_pair =
      gen.generate_two_predicate_join_tables(chunk_size, fact_table_size, fact_factor, probing_factor);

  // std::cout << join_pair->first->row_count() << std::endl;
  // std::cout << join_pair->second->row_count() << std::endl;

  const auto table_wrapper_left = std::make_shared<TableWrapper>(join_pair->first);
  table_wrapper_left->execute();
  const auto table_wrapper_right = std::make_shared<TableWrapper>(join_pair->second);
  table_wrapper_right->execute();

  std::vector<JoinPredicate> join_predicates;
  join_predicates.emplace_back(JoinPredicate{ColumnIDPair{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join_predicates.emplace_back(JoinPredicate{ColumnIDPair{ColumnID{1}, ColumnID{1}}, PredicateCondition::Equals});

  bm_join_impl(state, table_wrapper_left, table_wrapper_right, join_predicates);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To5)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 5);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To2Point5)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 5);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To10)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 10);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To5)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 5);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To100)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 100);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To50)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 50);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_5To20)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 5, 20);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_10To10)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 10, 10);
}

}  // namespace opossum
