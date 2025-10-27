#include <iostream>

// This playground only compiles on Linux as we require `perf`.
#include "benchmark_config.hpp"
#include "expression/expression_functional.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/reduce.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "perfcpp/event_counter.h"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"

using namespace hyrise;                 // NOLINT(build/namespaces)
using namespace expression_functional;  // NOLINT(build/namespaces)

int main() {
  float scale_factor = 0.1f;

  const char* env = std::getenv("SF");
  if (env) {
    scale_factor = std::stof(env);
  }

  auto& sm = Hyrise::get().storage_manager;
  const auto benchmark_config = std::make_shared<BenchmarkConfig>();

  std::cout << "Generating TPC-H data set with scale factor " << scale_factor << " and automatic encoding:\n";
  TPCHTableGenerator(scale_factor, ClusteringConfiguration::None, benchmark_config).generate_and_store();

  if (sm.has_table("lineitem")) {
    std::cout << "TPC-H lineitem table successfully generated with " << sm.get_table("lineitem")->row_count()
              << " rows.\n";
  }
  Assert(sm.has_table("lineitem"), "Something went wrong during TPC-H data generation");

  const auto pruned_chunk_ids = std::vector<ChunkID>{};
  const auto pruned_column_ids_lineorder = std::vector<ColumnID>{
      ColumnID{0},  ColumnID{2},  ColumnID{3},  ColumnID{6},  ColumnID{7},  ColumnID{8}, ColumnID{9},
      ColumnID{10}, ColumnID{11}, ColumnID{12}, ColumnID{13}, ColumnID{14}, ColumnID{15}};

  auto get_table_lineitem = std::make_shared<GetTable>("lineitem", pruned_chunk_ids, pruned_column_ids_lineorder);
  get_table_lineitem->never_clear_output();
  get_table_lineitem->execute();

  std::cout << "get_table_lineitem has " << get_table_lineitem->get_output()->row_count() << " rows.\n";

  const auto pruned_column_ids_part =
      std::vector<ColumnID>{ColumnID{1}, ColumnID{2}, ColumnID{4}, ColumnID{5}, ColumnID{7}, ColumnID{8}};

  auto get_table_part = std::make_shared<GetTable>("part", pruned_chunk_ids, pruned_column_ids_part);
  get_table_part->never_clear_output();
  get_table_part->execute();

  std::cout << "get_table_part has " << get_table_part->get_output()->row_count() << " rows.\n";

  const auto operand0 = pqp_column_(ColumnID{2}, get_table_part->get_output()->column_data_type(ColumnID{2}),
                                    get_table_part->get_output()->column_is_nullable(ColumnID{2}),
                                    get_table_part->get_output()->column_name(ColumnID{2}));
  const auto predicate0 =
      std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand0, value_("JUMBO CASE"));

  auto table_scan0 = std::make_shared<TableScan>(get_table_part, predicate0);
  table_scan0->never_clear_output();
  table_scan0->execute();

  std::cout << "table_scan0 has " << table_scan0->get_output()->row_count() << " rows.\n";

  auto operand1 = pqp_column_(ColumnID{1}, table_scan0->get_output()->column_data_type(ColumnID{1}),
                              table_scan0->get_output()->column_is_nullable(ColumnID{1}),
                              table_scan0->get_output()->column_name(ColumnID{1}));
  const auto predicate1 =
      std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand1, value_("Brand#11"));

  auto table_scan1 = std::make_shared<TableScan>(table_scan0, predicate1);
  table_scan1->never_clear_output();
  table_scan1->execute();

  std::cout << "table_scan1 has " << table_scan1->get_output()->row_count() << " rows.\n";

  /**
   * The following usage of perf-cpp is just to show case how to use these tools. Feel free to add helper
   * methods, classes, ... whatever you need.
   *
   * Initialize the perf-cpp counters.
   */
  auto counters = perf::CounterDefinition{};
  auto event_counter = perf::EventCounter{counters};

  std::cout << "\n scale factor: " << scale_factor << "\n";

  // Specify hardware events to count.
  event_counter.add({"seconds", "instructions", "cycles", "cache-misses", "dTLB-miss-ratio"});

  
  // Semi-join reduction: p_partkey (column 0) = l_partkey (column 0)
  const auto join_predicate = OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals};
  auto semi_join = std::make_shared<JoinHash>(get_table_lineitem, table_scan1, JoinMode::Semi, join_predicate);

  event_counter.start();
  semi_join->execute();
  event_counter.stop();

  std::cout << "Semi-join output has " << semi_join->get_output()->row_count() << " rows.\n";

  auto perf_result = event_counter.result();
  for (const auto& [event_name, value] : perf_result) {
    std::cout << event_name << ": " << value << '\n';
  }
  std::cout << '\n';

  // Build phase: build reduction structure from filtered part table
  auto build_reduce =
      std::make_shared<Reduce>(get_table_lineitem, table_scan1, join_predicate, ReduceMode::Build, UseMinMax::No);

  // Probe phase: reduce lineitem table using the built structure
  auto probe_reduce =
      std::make_shared<Reduce>(get_table_lineitem, build_reduce, join_predicate, ReduceMode::Probe, UseMinMax::No);

  event_counter.start();
  build_reduce->execute();
  probe_reduce->execute();
  event_counter.stop();

  std::cout << "Reduce output has " << probe_reduce->get_output()->row_count() << " rows.\n";

  // Print the results.
  perf_result = event_counter.result();
  for (const auto& [event_name, value] : perf_result) {
    std::cout << event_name << ": " << value << '\n';
  }
  std::cout << '\n';

  return 0;
}
