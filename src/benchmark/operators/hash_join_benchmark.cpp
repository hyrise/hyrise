
#include <algorithm>
#include <memory>

#include "column_generator.hpp"
#include "types.hpp"

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "concurrency/transaction_context.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/join_hash.hpp"
#include "operators/multi_predicate_join_evaluator.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/storage_manager.hpp"
#include "storage/value_segment.hpp"
#include "type_cast.hpp"

namespace {

const size_t CHUNK_SIZE = 10'000;
const size_t SMALL_TABLE_ROW_COUNT = 100'000;

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
      left, right, mode, join_predicates[0].column_id_pair, join_predicates[0].predicate_condition);
  latest_operator->execute();

  // execute table scans for the following predicates (ColumnVsColumnTableScan)
  for (size_t index = 1; index < join_predicates.size(); ++index) {
    const auto left_column_expr =
        PQPColumnExpression::from_table(*latest_operator->get_output(), join_predicates[index].column_id_pair.first);
    const auto right_column_expr = PQPColumnExpression::from_table(
        *latest_operator->get_output(),
        static_cast<ColumnID>(left->get_output()->column_count() + join_predicates[index].column_id_pair.second));
    const auto predicate = std::make_shared<BinaryPredicateExpression>(join_predicates[index].predicate_condition,
                                                                       left_column_expr, right_column_expr);
    latest_operator = std::make_shared<TableScan>(latest_operator, predicate);
    latest_operator->execute();
  }
}

void execute_multi_predicate_join(const std::shared_ptr<const AbstractOperator>& left,
                                  const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                  const std::vector<JoinPredicate>& join_predicates) {
  const std::vector<JoinPredicate> additional_predicates(join_predicates.cbegin() + 1, join_predicates.cend());

  // execute join for the first join predicate
  std::shared_ptr<AbstractOperator> latest_operator = std::make_shared<JoinHash>(
      left, right, mode, join_predicates[0].column_id_pair, join_predicates[0].predicate_condition, std::nullopt,
      std::vector<JoinPredicate>(additional_predicates));
  latest_operator->execute();
}

/**
 * Creates a benchmark set for a multi predicate join. table 1 will contain fact_table_size many rows.
 * Table 2 will contain fact_factor * probing_factor * fact_table_size many rows,
 * The intermediate result (after the first join) will contain fact_factor * fact_factor * probing_factor * fact_table_size many results.
 * The joined table will contain fact_factor * probing_factor rows again.
 */
void execute_multi_predicate_join(benchmark::State& state, size_t chunk_size, size_t fact_table_size,
                                  size_t fact_factor, double probing_factor, bool with_scan,
                                  bool as_reference_segment) {
  ColumnGenerator gen;

  const auto join_pair =
      gen.generate_two_predicate_join_tables(chunk_size, fact_table_size, fact_factor, probing_factor);

  const auto table_wrapper_left = std::make_shared<TableWrapper>(join_pair->first);
  table_wrapper_left->execute();
  const auto table_wrapper_right = std::make_shared<TableWrapper>(join_pair->second);
  table_wrapper_right->execute();

  std::vector<JoinPredicate> join_predicates;
  join_predicates.emplace_back(JoinPredicate{ColumnIDPair{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});

  clear_cache();

  std::shared_ptr<AbstractOperator> validate_left;
  std::shared_ptr<AbstractOperator> validate_right;

  if (as_reference_segment) {
    auto context = std::make_shared<TransactionContext>(1u, 3u);

    validate_left = std::make_shared<Validate>(table_wrapper_left);
    validate_right = std::make_shared<Validate>(table_wrapper_right);

    validate_left->set_transaction_context(context);
    validate_right->set_transaction_context(context);

    validate_left->execute();
    validate_right->execute();
  } else {
    validate_left = table_wrapper_left;
    validate_right = table_wrapper_right;
  }

  // Warm up
  if (with_scan) {
    execute_multi_predicate_join_with_scan(validate_left, validate_right, JoinMode::Inner, join_predicates);
  } else {
    execute_multi_predicate_join(validate_left, validate_right, JoinMode::Inner, join_predicates);
  }

  for (auto _ : state) {
    if (with_scan) {
      execute_multi_predicate_join_with_scan(validate_left, validate_right, JoinMode::Inner, join_predicates);
    } else {
      execute_multi_predicate_join(validate_left, validate_right, JoinMode::Inner, join_predicates);
    }
  }

  opossum::StorageManager::get().reset();
}

//---------------------------------------------------------------------------------------------------------------------
/* Multi predicate join using a scan for the second predicate and value segments */

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To5_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 1, 5, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To2Point5_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 2, 2.5, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To10_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 1, 10, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To5_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 2, 5, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To100_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 1, 100, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To50_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 2, 50, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_5To20_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 5, 20, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_10To10_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 10, 10, true, false);
}

//---------------------------------------------------------------------------------------------------------------------
/* True multi predicate join using value segments */

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To5_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 1, 5, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To2Point5_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 2, 2.5, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To10_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 1, 10, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To5_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 2, 5, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To100_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 1, 100, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To50_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 2, 50, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_5To20_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 5, 20, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_10To10_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 10, 10, false, false);
}

//---------------------------------------------------------------------------------------------------------------------
/* Multi predicate join using a scan for the second predicate and reference segments */

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To5_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 1, 5, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To2Point5_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 2, 2.5, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To10_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 1, 10, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To5_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 2, 5, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To100_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 1, 100, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To50_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 2, 50, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_5To20_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 5, 20, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_10To10_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 10, 10, true, true);
}

//---------------------------------------------------------------------------------------------------------------------
/* True multi predicate join using reference segments */

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To5_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 1, 5, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To2Point5_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 2, 2.5, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To10_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 1, 10, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To5_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 2, 5, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To100_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 1, 100, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To50_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 2, 50, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_5To20_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 5, 20, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_10To10_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SMALL_TABLE_ROW_COUNT, 10, 10, false, true);
}

}  // namespace opossum
