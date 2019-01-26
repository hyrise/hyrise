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
#include "storage/value_segment.hpp"
#include "type_cast.hpp"

namespace {

const size_t CHUNK_SIZE = 10000;
const size_t SCALE_FACTOR = 1'000'000;

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
    const auto predicate = std::make_shared<BinaryPredicateExpression>(join_predicates[index].predicate_condition,
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
      left, right, mode, join_predicates[0].column_id_pair, join_predicates[0].predicate_condition, std::nullopt,
      std::optional<std::vector<JoinPredicate>>(additional_predicates));
  latest_operator->execute();

  /*Print::print(left);
  Print::print(right);
  Print::print(latest_operator);

  std::cout << "Row count after predicates: "
            << latest_operator->get_output()->row_count() << std::endl;*/
}

/**
 * Creates a benchmark set for a multi predicate join. table 1 will contain fact_table_size many rows.
 * Table 2 will contain fact_factor * probing_factor many rows,
 * The intermediate result (after the first join) will contain fact_factor * fact_factor * probing_factor many results.
 * The joined table will contain fact_factor * probing_factor rows again.
 */
void execute_multi_predicate_join(benchmark::State& state, size_t chunk_size, size_t fact_table_size,
                                  size_t fact_factor, double probing_factor, bool with_scan,
                                  bool as_reference_segment) {
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

  std::shared_ptr<AbstractOperator> validate_left;
  std::shared_ptr<AbstractOperator> validate_right;

  if (as_reference_segment) {
    // validate needs a transaction context. This is taken from test/operators/validate_test.cpp:60
    auto context = std::make_shared<TransactionContext>(1u, 3u);

    // src/lib/operators/validate.cpp:121
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

  while (state.KeepRunning()) {
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
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 5, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To2Point5_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 2.5, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To10_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 10, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To5_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 5, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To100_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 100, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To50_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 50, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_5To20_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 5, 20, true, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_10To10_with_scan_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 10, 10, true, false);
}

//---------------------------------------------------------------------------------------------------------------------
/* True multi predicate join using value segments */

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To5_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 5, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To2Point5_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 2.5, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To10_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 10, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To5_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 5, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To100_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 100, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To50_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 50, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_5To20_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 5, 20, false, false);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_10To10_using_value_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 10, 10, false, false);
}

//---------------------------------------------------------------------------------------------------------------------
/* Multi predicate join using a scan for the second predicate and reference segments */

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To5_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 5, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To2Point5_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 2.5, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To10_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 10, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To5_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 5, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To100_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 100, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To50_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 50, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_5To20_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 5, 20, true, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_10To10_with_scan_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 10, 10, true, true);
}

//---------------------------------------------------------------------------------------------------------------------
/* True multi predicate join using reference segments */

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To5_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 5, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To2Point5_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 2.5, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To10_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 10, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To5_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 5, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_1To100_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 1, 100, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_2To50_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 2, 50, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_5To20_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 5, 20, false, true);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Multi_Predicate_Join_10To10_using_reference_segments)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  execute_multi_predicate_join(state, CHUNK_SIZE, SCALE_FACTOR, 10, 10, false, true);
}


void do_vs_benchmark(bool seq_access, bool use_all_type_variant, benchmark::State& state) {
  ValueSegment<int32_t> segment(false);
  segment.reserve(SCALE_FACTOR);

  std::vector<size_t> access_pattern;
  access_pattern.reserve(SCALE_FACTOR);

  Assert(RAND_MAX >= SCALE_FACTOR, "RAND_MAX to small");

  for (auto i = 0u; i < SCALE_FACTOR; ++i) {
    if (!seq_access) {
      access_pattern.push_back(rand() % SCALE_FACTOR);
    } else {
      access_pattern.push_back(i);
    }
  }

  for (auto i = 0; i < (int)SCALE_FACTOR; ++i) {
    segment.append(i);
  }

  long sum = 0;

  if (!use_all_type_variant) {
    while (state.KeepRunning()) {
      for (auto i = 0u; i < SCALE_FACTOR; ++i) {
        sum += segment.get(access_pattern[i]);
      }
    }
  } else {
    while (state.KeepRunning()) {
      for (auto i = 0u; i < SCALE_FACTOR; ++i) {
        sum += type_cast_variant<long>(segment[access_pattern[i]]);
      }
    }
  }
}

void do_vs_benchmark_table(bool seq_access, bool use_all_type_variant, benchmark::State& state) {
  const size_t CHUNK_SIZE = 100'000;

  Assert(SCALE_FACTOR % CHUNK_SIZE == 0, "CHUNK_SIZE must divide SCALE_FACTOR");

  const size_t chunk_count = (SCALE_FACTOR / CHUNK_SIZE);

  Table data_table({TableColumnDefinition("a", DataType::Int, false)}, TableType::Data, CHUNK_SIZE);

  std::vector<RowID> access_pattern;
  access_pattern.reserve(SCALE_FACTOR);

  Assert(RAND_MAX >= SCALE_FACTOR, "RAND_MAX to small");

  if (seq_access) {
    ChunkID chunk_id{0};
    ChunkOffset chunk_offset{0};

    for (size_t i = 0; i < SCALE_FACTOR; ++i) {
      access_pattern.emplace_back(chunk_id, chunk_offset);

      ++chunk_offset;
      if (chunk_offset == CHUNK_SIZE) {
        ++chunk_id;
        chunk_offset = 0;
      }
    }
  } else {
    ChunkID chunk_id{(uint32_t)(rand() % chunk_count)};
    ChunkOffset chunk_offset((uint32_t)(rand() % CHUNK_SIZE));

    for (size_t i = 0; i < SCALE_FACTOR; ++i) {
      access_pattern.emplace_back(chunk_id, chunk_offset);

      chunk_id = rand() % chunk_count;
      chunk_offset = (uint32_t)(rand() % CHUNK_SIZE);
    }
  }

  for (auto i = 0; i < (int)SCALE_FACTOR; ++i) {
    data_table.append({i});
  }

  long sum = 0;

  if (use_all_type_variant) {
    while (state.KeepRunning()) {
      for (auto i = 0u; i < SCALE_FACTOR; ++i) {
        const RowID& row = access_pattern[i];
        sum += type_cast_variant<long>(data_table.get_chunk(row.chunk_id)->get_segment(ColumnID{0})->operator[](row.chunk_offset));
      }
    }
  } else {
    while (state.KeepRunning()) {
      for (auto i = 0u; i < SCALE_FACTOR; ++i) {
        const RowID& row = access_pattern[i];
        const ValueSegment<int> *segment = static_cast<ValueSegment<int> *>(data_table.get_chunk(
            row.chunk_id)->get_segment(ColumnID{0}).get());
        sum += segment->get(row.chunk_offset);
      }
    }
  }
}


BENCHMARK_F(MicroBenchmarkBasicFixture, BM_SegmentAccess_VS_Seq_AllTypeVariant)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  do_vs_benchmark(true, true, state);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_SegmentAccess_VS_Rnd_AllTypeVariant)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  do_vs_benchmark(false, true, state);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_SegmentAccess_VS_Seq_Direct)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  do_vs_benchmark(true, false, state);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_SegmentAccess_VS_Rnd_Direct)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  do_vs_benchmark(false, false, state);
}


BENCHMARK_F(MicroBenchmarkBasicFixture, BM_SegmentAccess_VS_Seq_AllTypeVariant_Table)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  do_vs_benchmark_table(true, true, state);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_SegmentAccess_VS_Rnd_AllTypeVariant_Table)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  do_vs_benchmark_table(false, true, state);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_SegmentAccess_VS_Seq_Direct_Table)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  do_vs_benchmark_table(true, false, state);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_SegmentAccess_VS_Rnd_Direct_Table)
(benchmark::State& state) {  // NOLINT 1,000 x 1,000
  do_vs_benchmark_table(false, false, state);
}

//BENCHMARK_F(MicroBenchmarkBasicFixture, BM_SegmentAccess_RS_Seq_AllTypeVariant)
//(benchmark::State& state) {  // NOLINT 1,000 x 1,000

//}

}  // namespace opossum
