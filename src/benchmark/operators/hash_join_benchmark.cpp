#include <memory>

#include "column_generator.hpp"
#include "types.hpp"

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "concurrency/transaction_context.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/join_hash.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/storage_manager.hpp"

namespace {

const size_t CHUNK_SIZE = 10'000;
const size_t SCALE_FACTOR = 6;

void clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (int& i : clear) {
    i += 1;
  }
  clear.resize(0);
}

}  // namespace

namespace opossum {

void execute_multi_predicate_join_with_scan(const std::shared_ptr<const AbstractOperator>& left,
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

void execute_multi_predicate_join(const std::shared_ptr<const AbstractOperator>& left,
                                  const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                  const std::vector<JoinPredicate>& join_predicates) {
  const std::vector<JoinPredicate> additional_predicates(join_predicates.cbegin() + 1, join_predicates.cend());

  // execute join for the first join predicate
  std::shared_ptr<AbstractOperator> latest_operator = std::make_shared<JoinHash>(
      left, right, mode, join_predicates[0].column_id_pair, join_predicates[0].predicateCondition, std::nullopt,
      std::optional<std::vector<JoinPredicate>>(additional_predicates));
  latest_operator->execute();

  // Print::print(left);
  // Print::print(right);
  // Print::print(latest_operator);

  // Print::print(latest_operator);
  // std::cout << "Row count after " << index + 1 << " predicate: "
  //           << latest_operator->get_output()->row_count() << std::endl;
}

void execute_multi_predicate_join(benchmark::State& state, size_t chunk_size, size_t fact_table_size,
                                  size_t fact_factor, double probing_factor, bool with_scan) {
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

  clear_cache();

  //  Print::print(table_wrapper_left);
  //  Print::print(table_wrapper_right);

  /*
  // validate needs a transaction context. This is taken from test/operators/validate_test.cpp:60
  // not sure if this is the right thing to do here
  auto context = std::make_shared<TransactionContext>(1u, 3u);

  // this currently fails since the Tables have no MVCC data
  // src/lib/operators/validate.cpp:121
  std::shared_ptr<AbstractOperator> validate_left = std::make_shared<Validate>(table_wrapper_left);
  std::shared_ptr<AbstractOperator> validate_right = std::make_shared<Validate>(table_wrapper_right);

  validate_left->set_transaction_context(context);
  validate_right->set_transaction_context(context);

  validate_left->execute();
  validate_right->execute();
   */

  const auto& validate_left = table_wrapper_left;
  const auto& validate_right = table_wrapper_right;

  // Warm up
  if (with_scan) {
    execute_multi_predicate_join_with_scan(validate_left, validate_right, JoinMode::Inner, join_predicates);
  } else {
    execute_multi_predicate_join(validate_left, validate_right, JoinMode::Inner, join_predicates);
  }

  while (state.KeepRunning()) {
    if (with_scan) {
      execute_multi_predicate_join_with_scan(validate_left, validate_right, JoinMode::Inner, join_predicates);
    } else {
      execute_multi_predicate_join(validate_left, validate_right, JoinMode::Inner, join_predicates);
    }
  }

  opossum::StorageManager::get().reset();
}

/*BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To5_with_scan)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 5, true);
}*/

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To2Point5_with_scan)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 5, true);
}

/*BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To10_with_scan)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 10, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To5_with_scan)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 5, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To100_with_scan)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 100, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To50_with_scan)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 50, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_5To20_with_scan)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 5, 20, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_10To10_with_scan)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 10, 10, true);
}*/

////////////////////////////////////////////////////////////////////////////////////////
// Benchmark real multi predicate join
/*BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To5)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 5, false);
}*/

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To2Point5)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 5, false);
}
/*
BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To10)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 10, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To5)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 5, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To100)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 100, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To50)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 50, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_5To20)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 5, 20, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_10To10)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 10, 10, false);
}*/

}  // namespace opossum
