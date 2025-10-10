#include <memory>

#include "benchmark_config.hpp"
#include "expression/expression_functional.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/legacy_reduce.hpp"
#include "operators/reduce.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "ssb/ssb_table_generator.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "storage/encoding_type.hpp"
#include "tpch/tpch_constants.hpp"
#include "types.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

// Defining the base fixture class.
class ReductionBenchmarks : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    auto& sm = Hyrise::get().storage_manager;

    // Arg 0: scale factor
    const auto worker_count = static_cast<uint32_t>(state.range(0));
    const auto scale_factor = static_cast<uint8_t>(state.range(1));

    if (last_scale_factor != scale_factor) {
      // std::cout << "last: " << static_cast<int>(last_scale_factor) << " new: " << static_cast<int>(scale_factor) << "\n";
      // Reset (clears all tables)
      Hyrise::reset();
      generate_ssb_data(static_cast<float>(scale_factor), worker_count);
      last_scale_factor = scale_factor;
    }
    Assert(sm.has_table("lineorder"), "Something went wrong during SSB data generation");

    Hyrise::get().topology.use_default_topology(worker_count);
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  }

  // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
  void TearDown(::benchmark::State& /*state*/) override {
    Hyrise::get().scheduler()->finish();
  }

  void generate_ssb_data(const float scale_factor, const uint32_t worker_count) {
    const auto benchmark_config = std::make_shared<BenchmarkConfig>();
    std::cout << "Generating Star Schema data set with scale factor " << scale_factor
              << ", automatic encoding and worker count " << worker_count << "\n";

    const auto executable_path = std::filesystem::current_path();
    const auto ssb_dbgen_path = executable_path / "third_party/ssb-dbgen";
    Assert(std::filesystem::exists(ssb_dbgen_path / "dbgen"),
           std::string{"SSB dbgen not found at "} + ssb_dbgen_path.c_str());

    const auto query_path = executable_path / "../resources/benchmark/ssb/queries";
    const auto csv_meta_path = executable_path / "../resources/benchmark/ssb/schema";

    // Create the ssb_data directory (if needed) and generate the ssb_data/sf-... path.
    auto ssb_data_path_str = std::stringstream{};
    ssb_data_path_str << "ssb_data/sf-" << std::noshowpoint << scale_factor;
    std::filesystem::create_directories(ssb_data_path_str.str());
    // Success of create_directories is guaranteed by the call to fs::canonical, which fails on invalid paths.
    const auto ssb_data_path = std::filesystem::canonical(ssb_data_path_str.str());

    std::cout << "- Using SSB dbgen from " << ssb_dbgen_path << '\n';
    std::cout << "- Storing SSB tables in " << ssb_data_path << '\n';

    std::make_unique<SSBTableGenerator>(ssb_dbgen_path, csv_meta_path, ssb_data_path, scale_factor, benchmark_config)
        ->generate_and_store();
  }

  void setup_reduction_q41() {
    // Benchmarks the semi-join in query 4.1 of the SSB benchmark with a selectivity of 1.
    // It joins lineorder.lo_orderdate with date.d_datekey.

    // lineorder:
    // 00 LO_ORDERKEY      pruned
    // 01 LO_LINENUMBER    pruned
    // 02 LO_CUSTKEY       0
    // 03 LO_PARTKEY       1
    // 04 LO_SUPPKEY       2
    // 05 LO_ORDERDATE     3
    // 06 LO_ORDERPRIORITY pruned
    // 07 LO_SHIPPRIORITY  pruned
    // 08 LO_QUANTITY      pruned
    // 09 LO_EXTENDEDPRICE pruned
    // 10 LO_ORDTOTALPRICE pruned
    // 11 LO_DISCOUNT      pruned
    // 12 LO_REVENUE       4
    // 13 LO_SUPPLYCOST    5
    // 14 LO_TAX           pruned
    // 15 LO_COMMITDATE    pruned
    // 16 LO_SHIPMODE      pruned

    // date:
    // 00 D_DATEKEY          0
    // 01 D_DATE             pruned
    // 02 D_DAYOFWEEK        pruned
    // 03 03 D_MONTH         pruned
    // 04 D_YEAR             1
    // 05 D_YEARMONTHNUM     pruned
    // 06 D_YEARMONTH        pruned
    // 07 D_DAYNUMINWEEK     pruned
    // 08 D_DAYNUMINMONTH    pruned
    // 09 D_DAYNUMINYEAR     pruned
    // 10 D_MONTHNUMINYEAR   pruned
    // 11 D_WEEKNUMINYEAR    pruned
    // 12 D_SELLINGSEASON    pruned
    // 13 D_LASTDAYINWEEKFL  pruned
    // 14 D_LASTDAYINMONTHFL pruned
    // 15 D_HOLIDAYFL        pruned
    // 16 D_WEEKDAYFL        pruned

    const auto pruned_chunk_ids = std::vector<ChunkID>{};
    const auto pruned_column_ids_lineorder =
        std::vector<ColumnID>{ColumnID{0},  ColumnID{1},  ColumnID{6},  ColumnID{7},  ColumnID{8}, ColumnID{9},
                              ColumnID{10}, ColumnID{11}, ColumnID{14}, ColumnID{15}, ColumnID{16}};

    _left_input = std::make_shared<GetTable>("lineorder", pruned_chunk_ids, pruned_column_ids_lineorder);
    _left_input->never_clear_output();
    _left_input->execute();

    const auto pruned_column_ids_date = std::vector<ColumnID>{
        ColumnID{1},  ColumnID{2},  ColumnID{3},  ColumnID{5},  ColumnID{6},  ColumnID{7},  ColumnID{8}, ColumnID{9},
        ColumnID{10}, ColumnID{11}, ColumnID{12}, ColumnID{13}, ColumnID{14}, ColumnID{15}, ColumnID{16}};

    _right_input = std::make_shared<GetTable>("date", pruned_chunk_ids, pruned_column_ids_date);
    _right_input->never_clear_output();
    _right_input->execute();
  }

  void setup_reduction_q23() {
    // Benchmarks the semi-join in query 2.3 of the SSB benchmark with a selectivity of 0,00018. It joins
    // lineorder.lo_partkey with part.p_partkey. Before the join, this TableScan is applied: part.p_brand1 = "MFGR#2221".

    // lineorder:
    // 00 LO_ORDERKEY      pruned
    // 01 LO_LINENUMBER    pruned
    // 02 LO_CUSTKEY       pruned
    // 03 LO_PARTKEY       0
    // 04 LO_SUPPKEY       1
    // 05 LO_ORDERDATE     2
    // 06 LO_ORDERPRIORITY pruned
    // 07 LO_SHIPPRIORITY  pruned
    // 08 LO_QUANTITY      pruned
    // 09 LO_EXTENDEDPRICE pruned
    // 10 LO_ORDTOTALPRICE pruned
    // 11 LO_DISCOUNT      pruned
    // 12 LO_REVENUE       3
    // 13 LO_SUPPLYCOST    pruned
    // 14 LO_TAX           pruned
    // 15 LO_COMMITDATE    pruned
    // 16 LO_SHIPMODE      pruned

    // part:
    // 0 P_PARTKEY   0
    // 1 P_NAME      pruned
    // 2 P_MFGR      pruned
    // 3 P_CATEGORY  pruned
    // 4 P_BRAND     1
    // 5 P_COLOR     pruned
    // 6 P_TYPE      pruned
    // 7 P_SIZE      pruned
    // 8 P_CONTAINER pruned

    const auto pruned_chunk_ids = std::vector<ChunkID>{};
    const auto pruned_column_ids_lineorder =
        std::vector<ColumnID>{ColumnID{0}, ColumnID{1},  ColumnID{2},  ColumnID{6},  ColumnID{7},  ColumnID{8},
                              ColumnID{9}, ColumnID{10}, ColumnID{11}, ColumnID{14}, ColumnID{15}, ColumnID{16}};

    _left_input = std::make_shared<GetTable>("lineorder", pruned_chunk_ids, pruned_column_ids_lineorder);
    _left_input->never_clear_output();
    _left_input->execute();

    const auto pruned_column_ids_part = std::vector<ColumnID>{ColumnID{1}, ColumnID{2}, ColumnID{3}, ColumnID{5},
                                                              ColumnID{6}, ColumnID{7}, ColumnID{8}};

    const auto get_table_part = std::make_shared<GetTable>("part", pruned_chunk_ids, pruned_column_ids_part);
    get_table_part->never_clear_output();
    get_table_part->execute();

    const auto operand = pqp_column_(ColumnID{1}, get_table_part->get_output()->column_data_type(ColumnID{1}),
                                     get_table_part->get_output()->column_is_nullable(ColumnID{1}),
                                     get_table_part->get_output()->column_name(ColumnID{1}));
    const auto predicate =
        std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_("MFGR#2221"));

    _right_input = std::make_shared<TableScan>(get_table_part, predicate);
    _right_input->never_clear_output();
    _right_input->execute();

    const auto scan_selectivity = static_cast<double>(_right_input->get_output()->row_count()) /
                                  static_cast<double>(get_table_part->get_output()->row_count());
    Assert(scan_selectivity > 0.0005 && scan_selectivity < 0.002, "Selectivity:" + std::to_string(scan_selectivity));
  }

  void setup_reduction_q31() {
    // Benchmarks the semi-join in query 3.1 of the SSB benchmark with a selectivity of 0.91. It joins
    // lineorder.lo_orderdate with date.d_datekey. Before the join, this TableScan is applied to date:
    // d_year BETWEEN INCLUSIVE 1992 AND 1997.

    // lineorder:
    // 00 LO_ORDERKEY      pruned
    // 01 LO_LINENUMBER    pruned
    // 02 LO_CUSTKEY       0
    // 03 LO_PARTKEY       pruned
    // 04 LO_SUPPKEY       1
    // 05 LO_ORDERDATE     2
    // 06 LO_ORDERPRIORITY pruned
    // 07 LO_SHIPPRIORITY  pruned
    // 08 LO_QUANTITY      pruned
    // 09 LO_EXTENDEDPRICE pruned
    // 10 LO_ORDTOTALPRICE pruned
    // 11 LO_DISCOUNT      pruned
    // 12 LO_REVENUE       3
    // 13 LO_SUPPLYCOST    pruned
    // 14 LO_TAX           pruned
    // 15 LO_COMMITDATE    pruned
    // 16 LO_SHIPMODE      pruned

    // date:
    // 00 D_DATEKEY          0
    // 01 D_DATE             pruned
    // 02 D_DAYOFWEEK        pruned
    // 03 03 D_MONTH         pruned
    // 04 D_YEAR             1
    // 05 D_YEARMONTHNUM     pruned
    // 06 D_YEARMONTH        pruned
    // 07 D_DAYNUMINWEEK     pruned
    // 08 D_DAYNUMINMONTH    pruned
    // 09 D_DAYNUMINYEAR     pruned
    // 10 D_MONTHNUMINYEAR   pruned
    // 11 D_WEEKNUMINYEAR    pruned
    // 12 D_SELLINGSEASON    pruned
    // 13 D_LASTDAYINWEEKFL  pruned
    // 14 D_LASTDAYINMONTHFL pruned
    // 15 D_HOLIDAYFL        pruned
    // 16 D_WEEKDAYFL        pruned

    const auto pruned_chunk_ids = std::vector<ChunkID>{};
    const auto pruned_column_ids_lineorder = std::vector<ColumnID>{
        ColumnID{0},  ColumnID{1},  ColumnID{3},  ColumnID{6},  ColumnID{7},  ColumnID{8}, ColumnID{9},
        ColumnID{10}, ColumnID{11}, ColumnID{13}, ColumnID{14}, ColumnID{15}, ColumnID{16}};

    _left_input = std::make_shared<GetTable>("lineorder", pruned_chunk_ids, pruned_column_ids_lineorder);
    _left_input->never_clear_output();
    _left_input->execute();

    const auto pruned_column_ids_date = std::vector<ColumnID>{
        ColumnID{1},  ColumnID{2},  ColumnID{3},  ColumnID{5},  ColumnID{6},  ColumnID{7},  ColumnID{8}, ColumnID{9},
        ColumnID{10}, ColumnID{11}, ColumnID{12}, ColumnID{13}, ColumnID{14}, ColumnID{15}, ColumnID{16}};

    const auto get_table_date = std::make_shared<GetTable>("date", pruned_chunk_ids, pruned_column_ids_date);
    get_table_date->never_clear_output();
    get_table_date->execute();

    const auto operand = pqp_column_(ColumnID{1}, get_table_date->get_output()->column_data_type(ColumnID{1}),
                                     get_table_date->get_output()->column_is_nullable(ColumnID{1}),
                                     get_table_date->get_output()->column_name(ColumnID{1}));
    const auto predicate =
        std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1992), value_(1997));

    _right_input = std::make_shared<TableScan>(get_table_date, predicate);
    _right_input->never_clear_output();
    _right_input->execute();

    const auto scan_selectivity = static_cast<double>(_right_input->get_output()->row_count()) /
                                  static_cast<double>(get_table_date->get_output()->row_count());
    Assert(scan_selectivity > 0.8 && scan_selectivity < 0.9, "Selectivity:" + std::to_string(scan_selectivity));
  }

  std::shared_ptr<AbstractReadOnlyOperator> _left_input;
  std::shared_ptr<AbstractReadOnlyOperator> _right_input;

  // Remember last loaded scale factor across benchmark instances
  inline static uint8_t last_scale_factor{0};
};

// BENCHMARK_F(ReductionBenchmarks, WorstCaseSemiJoin)(benchmark::State& state) {
//   setup_reduction_q41();

//   Assert(_left_input && _right_input, "Left and right input must be set up before running the benchmark.");
//   Assert(_left_input->executed() && _right_input->executed(),
//          "Left and right input must be executed before running the benchmark.");

//   const auto join_dryrun = std::make_shared<JoinHash>(_left_input, _right_input, JoinMode::Semi,
//     OperatorJoinPredicate{ColumnIDPair(ColumnID{3}, ColumnID{0}), PredicateCondition::Equals});

//   join_dryrun->execute();
//   Assert(join_dryrun->get_output()->row_count() == _left_input->get_output()->row_count(),
//     "Semi join must not filter anything.");
//   state.counters["input_count"] = static_cast<double>(_left_input->get_output()->row_count());
//   state.counters["output_count"] = static_cast<double>(join_dryrun->get_output()->row_count());

//   for (auto _ : state) {
//     const auto join = std::make_shared<JoinHash>(_left_input, _right_input, JoinMode::Semi,
//       OperatorJoinPredicate{ColumnIDPair(ColumnID{3}, ColumnID{0}), PredicateCondition::Equals});
//     join->execute();
//   }
// }

// BENCHMARK_F(ReductionBenchmarks, BestCaseSemiJoin)(benchmark::State& state) {
//   setup_reduction_q23();

//   Assert(_left_input && _right_input, "Left and right input must be set up before running the benchmark.");
//   Assert(_left_input->executed() && _right_input->executed(),
//          "Left and right input must be executed before running the benchmark.");

//   const auto join_dryrun = std::make_shared<JoinHash>(_left_input, _right_input, JoinMode::Semi,
//     OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
//   join_dryrun->execute();

//   state.counters["input_count"] = static_cast<double>(_left_input->get_output()->row_count());
//   state.counters["output_count"] = static_cast<double>(join_dryrun->get_output()->row_count());

//   const auto join_selectivity = static_cast<double>(join_dryrun->get_output()->row_count()) /
//     static_cast<double>(_left_input->get_output()->row_count());
//   Assert(join_selectivity > 0.00005 && join_selectivity < 0.0015, "Selectivity:" + std::to_string(join_selectivity));

//   for (auto _ : state) {
//     const auto join = std::make_shared<JoinHash>(_left_input, _right_input, JoinMode::Semi,
//       OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
//     join->execute();
//   }
// }

// BENCHMARK_F(ReductionBenchmarks, BadCaseSemiJoin)(benchmark::State& state) {
//   setup_reduction_q31();

//   Assert(_left_input && _right_input, "Left and right input must be set up before running the benchmark.");
//   Assert(_left_input->executed() && _right_input->executed(),
//          "Left and right input must be executed before running the benchmark.");

//   const auto join_dryrun = std::make_shared<JoinHash>(_left_input, _right_input, JoinMode::Semi,
//     OperatorJoinPredicate{ColumnIDPair(ColumnID{2}, ColumnID{0}), PredicateCondition::Equals});
//   join_dryrun->execute();

//   state.counters["input_count"] = static_cast<double>(_left_input->get_output()->row_count());
//   state.counters["output_count"] = static_cast<double>(join_dryrun->get_output()->row_count());

//   const auto join_selectivity = static_cast<double>(join_dryrun->get_output()->row_count()) /
//     static_cast<double>(_left_input->get_output()->row_count());
//   Assert(join_selectivity > 0.86 && join_selectivity < 0.96, "Selectivity:" + std::to_string(join_selectivity));

//   for (auto _ : state) {
//     const auto join = std::make_shared<JoinHash>(
//         _left_input, _right_input, JoinMode::Semi,
//         OperatorJoinPredicate{ColumnIDPair(ColumnID{2}, ColumnID{0}), PredicateCondition::Equals});
//     join->execute();
//   }
// }

BENCHMARK_DEFINE_F(ReductionBenchmarks, WorstCaseReduce)(benchmark::State& state) {
  setup_reduction_q41();
  //   const auto filter_size = state.range(0);
  //   const auto hash_count = state.range(1);
  //   setenv("SIZE", std::to_string(filter_size).c_str(), 1);
  //   setenv("HASH", std::to_string(hash_count).c_str(), 1);

  Assert(_left_input && _right_input, "Left and right input must be set up before running the benchmark.");
  Assert(_left_input->executed() && _right_input->executed(),
         "Left and right input must be executed before running the benchmark.");

  const auto predicate = OperatorJoinPredicate{ColumnIDPair(ColumnID{3}, ColumnID{0}), PredicateCondition::Equals};
  const auto build_reduce_dryrun =
      std::make_shared<Reduce<ReduceMode::Build, UseMinMax::Yes>>(_left_input, _right_input, predicate);
  // const auto probe_reduce_dryrun =
  //     std::make_shared<Reduce<ReduceMode::Probe, UseMinMax::Yes>>(_left_input, build_reduce_dryrun, predicate);
  build_reduce_dryrun->execute();
  // probe_reduce_dryrun->execute();

  state.counters["input_count"] = static_cast<double>(_left_input->get_output()->row_count());
  // state.counters["output_count"] = static_cast<double>(probe_reduce_dryrun->get_output()->row_count());

  for (auto _ : state) {
    const auto build_reduce =
        std::make_shared<Reduce<ReduceMode::Build, UseMinMax::Yes>>(_left_input, _right_input, predicate);
    // const auto probe_reduce =
    //     std::make_shared<Reduce<ReduceMode::Probe, UseMinMax::Yes>>(_left_input, build_reduce, predicate);
    build_reduce->execute();
    // probe_reduce->execute();
  }
}

BENCHMARK_DEFINE_F(ReductionBenchmarks, BestCaseReduce)(benchmark::State& state) {
  setup_reduction_q23();
  //   const auto filter_size = state.range(0);
  //   const auto hash_count = state.range(1);
  //   setenv("SIZE", std::to_string(filter_size).c_str(), 1);
  //   setenv("HASH", std::to_string(hash_count).c_str(), 1);

  Assert(_left_input && _right_input, "Left and right input must be set up before running the benchmark.");
  Assert(_left_input->executed() && _right_input->executed(),
         "Left and right input must be executed before running the benchmark.");

  const auto predicate = OperatorJoinPredicate{ColumnIDPair(ColumnID{3}, ColumnID{0}), PredicateCondition::Equals};
  const auto build_reduce_dryrun =
      std::make_shared<Reduce<ReduceMode::Build, UseMinMax::Yes>>(_left_input, _right_input, predicate);
  const auto probe_reduce_dryrun =
      std::make_shared<Reduce<ReduceMode::Probe, UseMinMax::Yes>>(_left_input, build_reduce_dryrun, predicate);
  build_reduce_dryrun->execute();
  probe_reduce_dryrun->execute();

  state.counters["input_count"] = static_cast<double>(_left_input->get_output()->row_count());
  state.counters["output_count"] = static_cast<double>(probe_reduce_dryrun->get_output()->row_count());

  for (auto _ : state) {
    const auto build_reduce =
        std::make_shared<Reduce<ReduceMode::Build, UseMinMax::Yes>>(_left_input, _right_input, predicate);
    const auto probe_reduce =
        std::make_shared<Reduce<ReduceMode::Probe, UseMinMax::Yes>>(_left_input, build_reduce, predicate);
    build_reduce->execute();
    probe_reduce->execute();
  }
}

BENCHMARK_DEFINE_F(ReductionBenchmarks, BadCaseReduce)(benchmark::State& state) {
  setup_reduction_q31();
  const auto filter_size = state.range(0);
  const auto hash_count = state.range(1);
  setenv("SIZE", std::to_string(filter_size).c_str(), 1);
  setenv("HASH", std::to_string(hash_count).c_str(), 1);

  Assert(_left_input && _right_input, "Left and right input must be set up before running the benchmark.");
  Assert(_left_input->executed() && _right_input->executed(),
         "Left and right input must be executed before running the benchmark.");

  const auto predicate = OperatorJoinPredicate{ColumnIDPair(ColumnID{3}, ColumnID{0}), PredicateCondition::Equals};
  const auto build_reduce_dryrun =
      std::make_shared<Reduce<ReduceMode::Build, UseMinMax::Yes>>(_left_input, _right_input, predicate);
  const auto probe_reduce_dryrun =
      std::make_shared<Reduce<ReduceMode::Probe, UseMinMax::Yes>>(_left_input, build_reduce_dryrun, predicate);
  build_reduce_dryrun->execute();
  probe_reduce_dryrun->execute();

  state.counters["input_count"] = static_cast<double>(_left_input->get_output()->row_count());
  state.counters["output_count"] = static_cast<double>(probe_reduce_dryrun->get_output()->row_count());

  for (auto _ : state) {
    const auto build_reduce =
        std::make_shared<Reduce<ReduceMode::Build, UseMinMax::Yes>>(_left_input, _right_input, predicate);
    const auto probe_reduce =
        std::make_shared<Reduce<ReduceMode::Probe, UseMinMax::Yes>>(_left_input, build_reduce, predicate);
    build_reduce->execute();
    probe_reduce->execute();
  }
}

// BENCHMARK_F(ReductionBenchmarks, WorstCaseLegacy)(benchmark::State& state) {
//   setup_reduction_q41();

//   Assert(_left_input && _right_input, "Left and right input must be set up before running the benchmark.");
//   Assert(_left_input->executed() && _right_input->executed(),
//          "Left and right input must be executed before running the benchmark.");

//   const auto join_dryrun = std::make_shared<LegacyReduce>(_left_input, _right_input,
//       OperatorJoinPredicate{ColumnIDPair(ColumnID{3}, ColumnID{0}), PredicateCondition::Equals}, false);
//   join_dryrun->execute();

//   state.counters["input_count"] = static_cast<double>(_left_input->get_output()->row_count());
//   state.counters["output_count"] = static_cast<double>(join_dryrun->get_output()->row_count());

//   for (auto _ : state) {
//     const auto join = std::make_shared<LegacyReduce>(_left_input, _right_input,
//       OperatorJoinPredicate{ColumnIDPair(ColumnID{3}, ColumnID{0}), PredicateCondition::Equals}, false);
//     join->execute();
//   }
// }

// BENCHMARK_F(ReductionBenchmarks, BestCaseLegacy)(benchmark::State& state) {
//   setup_reduction_q23();

//   Assert(_left_input && _right_input, "Left and right input must be set up before running the benchmark.");
//   Assert(_left_input->executed() && _right_input->executed(),
//          "Left and right input must be executed before running the benchmark.");

//   const auto join_dryrun = std::make_shared<LegacyReduce>(_left_input, _right_input,
//       OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals}, false);
//   join_dryrun->execute();

//   state.counters["input_count"] = static_cast<double>(_left_input->get_output()->row_count());
//   state.counters["output_count"] = static_cast<double>(join_dryrun->get_output()->row_count());

//   for (auto _ : state) {
//     const auto join = std::make_shared<LegacyReduce>(_left_input, _right_input,
//       OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals}, false);
//     join->execute();
//   }
// }

// BENCHMARK_F(ReductionBenchmarks, BadCaseLegacy)(benchmark::State& state) {
//   setup_reduction_q31();

//   Assert(_left_input && _right_input, "Left and right input must be set up before running the benchmark.");
//   Assert(_left_input->executed() && _right_input->executed(),
//          "Left and right input must be executed before running the benchmark.");

//   const auto join_dryrun = std::make_shared<LegacyReduce>(
//         _left_input, _right_input,
//         OperatorJoinPredicate{ColumnIDPair(ColumnID{2}, ColumnID{0}), PredicateCondition::Equals}, false);
//   join_dryrun->execute();

//   state.counters["input_count"] = static_cast<double>(_left_input->get_output()->row_count());
//   state.counters["output_count"] = static_cast<double>(join_dryrun->get_output()->row_count());

//   for (auto _ : state) {
//     const auto join = std::make_shared<LegacyReduce>(
//         _left_input, _right_input,
//         OperatorJoinPredicate{ColumnIDPair(ColumnID{2}, ColumnID{0}), PredicateCondition::Equals}, false);
//     join->execute();
//   }
// }

BENCHMARK_REGISTER_F(ReductionBenchmarks, WorstCaseReduce)
    ->ArgsProduct({// {1, 2, 3, 4, 5, 7, 14, 28},
                   {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14,
                    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28},
                   {20}});

// BENCHMARK_REGISTER_F(ReductionBenchmarks, BestCaseReduce)
//     ->ArgsProduct({// {1, 2, 3, 4, 5, 7, 14, 28},
//                    {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14,
//                     15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28},
//                   //  {1, 5, 10, 20, 100}});
//                    {20}});

// BENCHMARK_REGISTER_F(ReductionBenchmarks, BadCaseReduce)
//     ->ArgsProduct({// {1, 2, 3, 4, 5, 7, 14, 28},
//                    {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14,
//                     15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28},
//                    {1, 5, 10, 20, 100}});

}  // namespace hyrise
