#include <fstream>
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
// using namespace expression_functional;  // NOLINT(build/namespaces)

const std::vector<uint8_t> filter_size_exponents = {0, 20};
const std::vector<uint8_t> block_size_exponents = {0, 9};
const std::vector<uint8_t> ks = {1, 2};

const uint16_t min_runs = 10;
// const uint16_t max_runs = 50;
const int64_t min_time_ns = 30'000'000'000;

struct BenchmarkResult {
  std::string operator_type;
  uint8_t filter_size_exponent;
  uint8_t block_size_exponent;
  uint8_t k;
  uint16_t run;
  int64_t total_time_ns;
};

void setup_tpch(const float scale_factor) {
  auto& sm = Hyrise::get().storage_manager;
  const auto benchmark_config = std::make_shared<BenchmarkConfig>();
  benchmark_config->cache_binary_tables = true;

  std::cout << "scale factor: " << scale_factor << "\n";
  TPCHTableGenerator(scale_factor, ClusteringConfiguration::None, benchmark_config).generate_and_store();

  if (sm.has_table("lineitem")) {
    std::cout << "lineitem rows: " << sm.get_table("lineitem")->row_count() << "\n";
  } else {
    Fail("TPC-H data generation failed");
  }
}

std::pair<std::shared_ptr<AbstractOperator>, std::shared_ptr<AbstractOperator>> setup_q17() {
  const auto pruned_chunk_ids = std::vector<ChunkID>{};

  const auto pruned_column_ids_lineitem = std::vector<ColumnID>{
      ColumnID{0},  ColumnID{2},  ColumnID{3},  ColumnID{6},  ColumnID{7},  ColumnID{8}, ColumnID{9},
      ColumnID{10}, ColumnID{11}, ColumnID{12}, ColumnID{13}, ColumnID{14}, ColumnID{15}};
  const auto get_table_lineitem = std::make_shared<GetTable>("lineitem", pruned_chunk_ids, pruned_column_ids_lineitem);
  get_table_lineitem->never_clear_output();

  get_table_lineitem->execute();
  std::cout << "get_table_lineitem rows: " << get_table_lineitem->get_output()->row_count() << "\n";

  const auto pruned_column_ids_part =
      std::vector<ColumnID>{ColumnID{1}, ColumnID{2}, ColumnID{4}, ColumnID{5}, ColumnID{7}, ColumnID{8}};
  const auto get_table_part = std::make_shared<GetTable>("part", pruned_chunk_ids, pruned_column_ids_part);

  get_table_part->execute();
  std::cout << "get_table_part rows: " << get_table_part->get_output()->row_count() << "\n";

  const auto operand0 = expression_functional::pqp_column_(ColumnID{2}, get_table_part->get_output()->column_data_type(ColumnID{2}),
                                    get_table_part->get_output()->column_is_nullable(ColumnID{2}),
                                    get_table_part->get_output()->column_name(ColumnID{2}));
  const auto predicate0 =
      std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand0, expression_functional::value_("JUMBO CASE"));
  const auto table_scan0 = std::make_shared<TableScan>(get_table_part, predicate0);

  table_scan0->execute();
  std::cout << "table_scan0 rows: " << table_scan0->get_output()->row_count() << "\n";

  const auto operand1 = expression_functional::pqp_column_(ColumnID{1}, table_scan0->get_output()->column_data_type(ColumnID{1}),
                              table_scan0->get_output()->column_is_nullable(ColumnID{1}),
                              table_scan0->get_output()->column_name(ColumnID{1}));
  const auto predicate1 =
      std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand1, expression_functional::value_("Brand#11"));
  const auto table_scan1 = std::make_shared<TableScan>(table_scan0, predicate1);
  table_scan1->never_clear_output();

  table_scan1->execute();
  std::cout << "table_scan1 rows: " << table_scan1->get_output()->row_count() << "\n";

  return {get_table_lineitem, table_scan1};
}

template <typename F>
auto measure_duration(F&& f) {
  auto start = std::chrono::high_resolution_clock::now();
  f();
  auto end = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();  // in ns
}

void perform_measurements(std::ofstream& out, OperatorJoinPredicate join_predicate, std::shared_ptr<AbstractOperator> left_input, std::shared_ptr<AbstractOperator> right_input, uint8_t filter_size_exponent,
                         uint8_t block_size_exponent, uint8_t k) {
  if (filter_size_exponent == 0 && (block_size_exponent != 0 || k != 1)) {
    return;
  }

  std::cout << "Measuring for filter_size_exponent=" << static_cast<int>(filter_size_exponent)
            << ", block_size_exponent=" << static_cast<int>(block_size_exponent) << ", k=" << static_cast<int>(k) << "\n";

  auto run = uint16_t{0};
  auto total_time = int64_t{0};
  auto duration_ns = int64_t{0};

  while ((run < min_runs || total_time < min_time_ns) /*&& run < max_runs*/) {
    if (filter_size_exponent == 0) {
      const auto semi_join = std::make_shared<JoinHash>(left_input, right_input, JoinMode::Semi, join_predicate);
      duration_ns = measure_duration([&]() {
        semi_join->execute();
      });
    } else {
      const auto reduce_build =
          std::make_shared<Reduce>(left_input, right_input, join_predicate, ReduceMode::Build, UseMinMax::No,
                                    filter_size_exponent, block_size_exponent, k);

      const auto reduce_probe =
          std::make_shared<Reduce>(left_input, reduce_build, join_predicate, ReduceMode::Probe, UseMinMax::No,
                                     filter_size_exponent, block_size_exponent, k);

      duration_ns = measure_duration([&]() {
        reduce_build->execute();
        reduce_probe->execute();
      });
    }

    total_time += duration_ns;

    out << static_cast<int>(filter_size_exponent) << ',' << static_cast<int>(block_size_exponent) << ','
        << static_cast<int>(k) << ',' << run++ << ',' << duration_ns << '\n';
  }
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <scale_factor> <perf|output_csv_file>\n";
    return 1;
  }

  const auto scale_factor = std::stof(argv[1]);
  const std::string second_arg = argv[2];

  const auto join_predicate = OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals};

  if (second_arg != "perf") {

  

  // Handle CSV output
  if (std::filesystem::exists(second_arg)) {
    std::cerr << "Error: File " << second_arg << " already exists.\n";
    return 1;
  }

  // Write CSV header
  std::ofstream out(second_arg);
  if (!out.is_open()) {
    std::cerr << "Error: Failed to create CSV file.\n";
    return 1;
  }

  out << "filter_size_exponent,block_size_exponent,k,run,time_ns\n";
  out.flush();

  setup_tpch(scale_factor);
  const auto [left_input, right_input] = setup_q17();

  for (const auto filter_size_exponent : filter_size_exponents) {
    for (const auto block_size_exponent : block_size_exponents) {
      for (const auto k : ks) {
        perform_measurements(out, join_predicate, left_input, right_input, filter_size_exponent, block_size_exponent, k);
      }
    }
  }

  out.close();

  } else {

  // auto counters = perf::CounterDefinition{};
  // auto event_counter = perf::EventCounter{counters};
  // (void)event_counter;

  // event_counter.add({
  //     "nanoseconds",
  //     "branch-miss-ratio",
  //     "cache-miss-ratio",
  //     "L1-data-miss-ratio",
  //     "dTLB-miss-ratio",
  //     "iTLB-miss-ratio",
  //     "instructions",
  //     "instructions-per-cycle",
  // });

  // Prepare CSV output with stable column order.
  // const std::vector<std::string> event_names = {
  //     "nanoseconds",     "branch-miss-ratio", "cache-miss-ratio", "L1-data-miss-ratio",
  //     "dTLB-miss-ratio", "iTLB-miss-ratio",   "instructions",     "instructions-per-cycle",
  // };
  // std::ofstream csv_out("perf_results.csv");
  // csv_out << "scenario,block_size_exponent,k";
  // for (const auto& name : event_names)
  //   csv_out << ',' << name;
  // csv_out << '\n';

  // auto write_csv_row = [&](const std::string& scenario, const std::string& bse, const std::string& k) {
  //   const auto perf_result = event_counter.result();
  //   csv_out << scenario << ',' << bse << ',' << k;
  //   for (const auto& name : event_names) {
  //     double value = 0.0;
  //     for (const auto& [event_name, event_value] : perf_result) {
  //       if (event_name == name) {
  //         value = event_value;
  //         break;
  //       }
  //     }
  //     csv_out << ',' << value;
  //   }
  //   csv_out << '\n';
  // };

  // Semi-join reduction: p_partkey (column 0) = l_partkey (column 0)
  // auto semi_join = std::make_shared<JoinHash>(left_input, right_input, JoinMode::Semi, join_predicate);

  // perform_measurement(out, 0, 0, 0, [&]() {
  //   semi_join->execute();
  // });

  // event_counter.start();
  // semi_join->execute();
  // event_counter.stop();

  // std::cout << "semi-join rows: " << semi_join->get_output()->row_count() << "\n";
  // std::cout << "Semi-join PerformanceData:\n";
  // semi_join->performance_data->output_to_stream(std::cout, DescriptionMode::MultiLine);

  // std::cout << "Semi-join perf_result:\n";
  // auto perf_result = event_counter.result();
  // for (const auto& [event_name, value] : perf_result) {
  //   std::cout << event_name << ": " << value << '\n';
  // }
  // std::cout << '\n';
  // // Write CSV row for Semi-join (no BSE/k)
  // write_csv_row("SemiJoin", "NA", "NA");

  // Measure Reduce (Build+Probe) for different configurations:
  // - filter_size_exponent fixed at 20
  // - block_size_exponent in {0, 9}
  // - k in {1, 2}
      // auto build_reduce =
      //     std::make_shared<Reduce>(left_input, right_input, join_predicate, ReduceMode::Build, UseMinMax::No,
      //                              /*filter_size_exponent*/ 20, 0, 2);

      // auto probe_reduce =
      //     std::make_shared<Reduce>(left_input, build_reduce, join_predicate, ReduceMode::Probe, UseMinMax::No,
      //                              /*filter_size_exponent*/ 20, 0, 2);

      // build_reduce->execute();
      // // event_counter.start();
      // probe_reduce->execute();
      // event_counter.stop();

      // std::cout << "Reduce Build+Probe (BSE=" << int(0) << ", k=" << int(2)
      //           << ") output: " << probe_reduce->get_output()->row_count() << " rows.\n";
      // std::cout << "Reduce Build+Probe PerformanceData (Build):\n";
      // build_reduce->performance_data->output_to_stream(std::cout, DescriptionMode::MultiLine);
      // std::cout << "Reduce Build+Probe PerformanceData (Probe):\n";
      // probe_reduce->performance_data->output_to_stream(std::cout, DescriptionMode::MultiLine);

      // std::cout << "Reduce Build+Probe perf_result:\n";
      // perf_result = event_counter.result();
      // for (const auto& [event_name, value] : perf_result) {
      //   std::cout << event_name << ": " << value << '\n';
      // }
      // std::cout << '\n';

      // CSV row for this configuration
      // write_csv_row("ReduceBuildProbe", std::to_string(block_size_exponent), std::to_string(k));

  // csv_out.close();
    std::cout << "perf measurements not yet implemented.\n";
  }

  return 0;
}
