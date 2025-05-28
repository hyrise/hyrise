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
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "storage/encoding_type.hpp"
#include "tpch/tpch_constants.hpp"
#include "ssb/ssb_table_generator.hpp"
#include "types.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class TableWrapper;

// Defining the base fixture class.
class StarSchemaDataMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    auto& sm = Hyrise::get().storage_manager;
    const auto scale_factor = 1.0f;
    const auto benchmark_config = std::make_shared<BenchmarkConfig>();

    if (!sm.has_table("lineorder")) {
      std::cout << "Generating Star Schema data set with scale factor " << scale_factor << " and "
                << benchmark_config->encoding_config.default_encoding_spec << " encoding:\n";

      // Try to find dbgen binary.
      const auto executable_path = std::filesystem::current_path();
      // Assert(1 == 2, executable_path);
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

      // Create the table generator and item runner.
      auto table_generator =
          std::make_unique<SSBTableGenerator>(ssb_dbgen_path, csv_meta_path, ssb_data_path, scale_factor, benchmark_config);
        table_generator->generate_and_store();
    }

    _table_wrapper_map = create_table_wrappers(sm);
  }

  // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
  void TearDown(::benchmark::State& /*state*/) override {}

  std::map<std::string, std::shared_ptr<TableWrapper>> create_table_wrappers(StorageManager& sm) {
    std::map<std::string, std::shared_ptr<TableWrapper>> wrapper_map;
    for (const auto& table_name : sm.table_names()) {
      auto table = sm.get_table(table_name);
      auto table_wrapper = std::make_shared<TableWrapper>(table);
      table_wrapper->never_clear_output();
      table_wrapper->execute();

      wrapper_map.emplace(table_name, table_wrapper);
    }

    return wrapper_map;
  }

  inline static bool _tpch_data_generated = false;

  std::map<std::string, std::shared_ptr<TableWrapper>> _table_wrapper_map;
};

BENCHMARK_F(StarSchemaDataMicroBenchmarkFixture, SemiJoinWorstCase)(benchmark::State& state) {
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
  const auto pruned_column_ids_lineorder = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}, ColumnID{6}, ColumnID{7}, ColumnID{8},
                                  ColumnID{9}, ColumnID{10}, ColumnID{11}, ColumnID{14}, ColumnID{15}, ColumnID{16}};

  const auto get_table_lineorder = std::make_shared<GetTable>("lineorder", pruned_chunk_ids, pruned_column_ids_lineorder);
  get_table_lineorder->never_clear_output();
  get_table_lineorder->execute();

  const auto pruned_column_ids_date = std::vector<ColumnID>{ColumnID{1}, ColumnID{2}, ColumnID{3}, ColumnID{5},
                                  ColumnID{6}, ColumnID{7}, ColumnID{8}, ColumnID{9}, ColumnID{10},
                                  ColumnID{11}, ColumnID{12}, ColumnID{13}, ColumnID{14}, ColumnID{15},
                                  ColumnID{16}};

  const auto get_table_date = std::make_shared<GetTable>("date", pruned_chunk_ids, pruned_column_ids_date);
  get_table_date->never_clear_output();
  get_table_date->execute();

  for (auto _ : state) {
    auto join = std::make_shared<JoinHash>(
        get_table_lineorder, get_table_date, JoinMode::Semi,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{3}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
    Assert(join->get_output()->row_count() == get_table_lineorder->get_output()->row_count(), "Semi join must not filter anything.");
  }
}

BENCHMARK_F(StarSchemaDataMicroBenchmarkFixture, SemiJoinBestCase)(benchmark::State& state) {
  // Benchmarks the semi-join in query 2.3 of the SSB benchmark with a selectivity of 0.001. It joins
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
  const auto pruned_column_ids_lineorder = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{6}, ColumnID{7},
                                  ColumnID{8}, ColumnID{9}, ColumnID{10}, ColumnID{11}, ColumnID{14}, ColumnID{15}, ColumnID{16}};

  const auto get_table_lineorder = std::make_shared<GetTable>("lineorder", pruned_chunk_ids, pruned_column_ids_lineorder);
  get_table_lineorder->never_clear_output();
  get_table_lineorder->execute();

  const auto pruned_column_ids_part = std::vector<ColumnID>{ColumnID{1}, ColumnID{2}, ColumnID{3}, ColumnID{5},
                                  ColumnID{6}, ColumnID{7}, ColumnID{8}};

  const auto get_table_part = std::make_shared<GetTable>("part", pruned_chunk_ids, pruned_column_ids_part);
  get_table_part->never_clear_output();
  get_table_part->execute();

  auto operand = pqp_column_(ColumnID{1}, get_table_part->get_output()->column_data_type(ColumnID{1}),
                                           get_table_part->get_output()->column_is_nullable(ColumnID{1}),
                                           get_table_part->get_output()->column_name(ColumnID{1}));
  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_("MFGR#2221"));
  const auto table_scan_part = std::make_shared<TableScan>(get_table_part, predicate);
  table_scan_part->never_clear_output();
  table_scan_part->execute();

  auto selectivity = static_cast<double>(table_scan_part->get_output()->row_count()) / static_cast<double>(get_table_part->get_output()->row_count());
  Assert(selectivity > 0.0005 && selectivity < 0.002, "Selectivity:" + std::to_string(selectivity));

  for (auto _ : state) {
    auto join = std::make_shared<JoinHash>(
        get_table_lineorder, table_scan_part, JoinMode::Semi,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
  }
}

BENCHMARK_F(StarSchemaDataMicroBenchmarkFixture, SemiJoin31)(benchmark::State& state) {
  // Benchmarks the semi-join in query 3.1 of the SSB benchmark with a selectivity of 0.85. It joins
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
  const auto pruned_column_ids_lineorder = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}, ColumnID{3}, ColumnID{6}, ColumnID{7}, ColumnID{8}, ColumnID{9},
                                  ColumnID{10}, ColumnID{11}, ColumnID{13}, ColumnID{14},
                                  ColumnID{15}, ColumnID{16}};

  const auto get_table_lineorder = std::make_shared<GetTable>("lineorder", pruned_chunk_ids, pruned_column_ids_lineorder);
  get_table_lineorder->never_clear_output();
  get_table_lineorder->execute();

  const auto pruned_column_ids_date = std::vector<ColumnID>{ColumnID{1}, ColumnID{2}, ColumnID{3}, ColumnID{5},
                                  ColumnID{6}, ColumnID{7}, ColumnID{8}, ColumnID{9}, ColumnID{10},
                                  ColumnID{11}, ColumnID{12}, ColumnID{13}, ColumnID{14}, ColumnID{15},
                                  ColumnID{16}};

  const auto get_table_date = std::make_shared<GetTable>("date", pruned_chunk_ids, pruned_column_ids_date);
  get_table_date->never_clear_output();
  get_table_date->execute();

  auto operand = pqp_column_(ColumnID{1}, get_table_date->get_output()->column_data_type(ColumnID{1}),
                                           get_table_date->get_output()->column_is_nullable(ColumnID{1}),
                                           get_table_date->get_output()->column_name(ColumnID{1}));

  auto predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1992), value_(1997));
  const auto table_scan_date = std::make_shared<TableScan>(get_table_date, predicate);
  table_scan_date->never_clear_output();
  table_scan_date->execute();

  auto selectivity = static_cast<double>(table_scan_date->get_output()->row_count()) / static_cast<double>(get_table_date->get_output()->row_count());
  Assert(selectivity > 0.8 && selectivity < 0.9, "Something is wrong.");

  for (auto _ : state) {
    auto join = std::make_shared<JoinHash>(
        get_table_lineorder, table_scan_date, JoinMode::Semi,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{2}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
  }
}


// BENCHMARK_REGISTER_F(StarSchemaDataMicroBenchmarkFixture, SemiJoinTest)->DenseRange(0, 15);

}  // namespace hyrise
