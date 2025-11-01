#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstddef>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark_config.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/join_hash.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class TableWrapper;

// Defining the base fixture class.
class TPCHDataMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& /*state*/) override {
    auto& storage_manager = Hyrise::get().storage_manager;
    const auto scale_factor = float{10};
    const auto benchmark_config = std::make_shared<BenchmarkConfig>();

    if (!storage_manager.has_table("lineitem")) {
      std::cout << "Generating TPC-H data set with scale factor " << scale_factor << " and automatic encoding:\n";
      TPCHTableGenerator(scale_factor, ClusteringConfiguration::None, benchmark_config).generate_and_store();
    }

    table_wrapper_map = create_table_wrappers(storage_manager);

    auto lineitem_table = storage_manager.get_table("lineitem");

    // Predicates as in TPC-H Q6, ordered by selectivity. Not necessarily the same order as determined by the optimizer
    tpchq6_discount_operand = pqp_column_(ColumnID{6}, lineitem_table->column_data_type(ColumnID{6}),
                                          lineitem_table->column_is_nullable(ColumnID{6}), "");
    tpchq6_discount_predicate = std::make_shared<BetweenExpression>(
        PredicateCondition::BetweenInclusive, tpchq6_discount_operand, value_(0.05), value_(0.70001));

    tpchq6_shipdate_less_operand = pqp_column_(ColumnID{10}, lineitem_table->column_data_type(ColumnID{10}),
                                               lineitem_table->column_is_nullable(ColumnID{10}), "");
    tpchq6_shipdate_less_predicate = std::make_shared<BinaryPredicateExpression>(
        PredicateCondition::LessThan, tpchq6_shipdate_less_operand, value_("1995-01-01"));

    tpchq6_quantity_operand = pqp_column_(ColumnID{4}, lineitem_table->column_data_type(ColumnID{4}),
                                          lineitem_table->column_is_nullable(ColumnID{4}), "");
    tpchq6_quantity_predicate =
        std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, tpchq6_quantity_operand, value_(24));

    // The following two "synthetic" predicates have a selectivity of 1.0
    lorderkey_operand = pqp_column_(ColumnID{0}, lineitem_table->column_data_type(ColumnID{0}),
                                    lineitem_table->column_is_nullable(ColumnID{0}), "");
    int_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals,
                                                                lorderkey_operand, value_(-5));

    lshipinstruct_operand = pqp_column_(ColumnID{13}, lineitem_table->column_data_type(ColumnID{13}),
                                        lineitem_table->column_is_nullable(ColumnID{13}), "");
    string_predicate =
        std::make_shared<BinaryPredicateExpression>(PredicateCondition::NotEquals, lshipinstruct_operand, value_("a"));

    orders_table_node = StoredTableNode::make("orders");
    orders_orderpriority = orders_table_node->get_column("o_orderpriority");
    orders_orderdate = orders_table_node->get_column("o_orderdate");
    orders_orderkey = orders_table_node->get_column("o_orderkey");

    lineitem_table_node = StoredTableNode::make("lineitem");
    lineitem_orderkey = lineitem_table_node->get_column("l_orderkey");
    lineitem_commitdate = lineitem_table_node->get_column("l_commitdate");
    lineitem_receiptdate = lineitem_table_node->get_column("l_receiptdate");
  }

  // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
  void TearDown(::benchmark::State& /*state*/) override {}

  static std::map<std::string, std::shared_ptr<TableWrapper>> create_table_wrappers(StorageManager& storage_manager) {
    std::map<std::string, std::shared_ptr<TableWrapper>> wrapper_map;
    for (const auto& table_name : storage_manager.table_names()) {
      auto table = storage_manager.get_table(table_name);
      auto table_wrapper = std::make_shared<TableWrapper>(table);
      table_wrapper->never_clear_output();
      table_wrapper->execute();

      wrapper_map.emplace(table_name, table_wrapper);
    }

    return wrapper_map;
  }

  inline static bool tpch_data_generated = false;

  std::map<std::string, std::shared_ptr<TableWrapper>> table_wrapper_map;

  std::shared_ptr<PQPColumnExpression> lorderkey_operand;
  std::shared_ptr<BinaryPredicateExpression> int_predicate;
  std::shared_ptr<PQPColumnExpression> lshipinstruct_operand;
  std::shared_ptr<BinaryPredicateExpression> string_predicate;

  std::shared_ptr<PQPColumnExpression> tpchq6_discount_operand;
  std::shared_ptr<BetweenExpression> tpchq6_discount_predicate;
  std::shared_ptr<PQPColumnExpression> tpchq6_shipdate_less_operand;
  std::shared_ptr<BinaryPredicateExpression> tpchq6_shipdate_less_predicate;
  std::shared_ptr<PQPColumnExpression> tpchq6_quantity_operand;
  std::shared_ptr<BinaryPredicateExpression> tpchq6_quantity_predicate;

  std::shared_ptr<StoredTableNode> orders_table_node, lineitem_table_node;
  std::shared_ptr<LQPColumnExpression> orders_orderpriority, orders_orderdate, orders_orderkey;
  std::shared_ptr<LQPColumnExpression> lineitem_orderkey, lineitem_commitdate, lineitem_receiptdate;
};

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ6FirstScanPredicate)(benchmark::State& state) {
  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper_map.at("lineitem"), tpchq6_discount_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ6SecondScanPredicate)(benchmark::State& state) {
  const auto first_scan = std::make_shared<TableScan>(table_wrapper_map.at("lineitem"), tpchq6_discount_predicate);
  first_scan->never_clear_output();
  first_scan->execute();

  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(first_scan, tpchq6_shipdate_less_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ6ThirdScanPredicate)(benchmark::State& state) {
  const auto first_scan = std::make_shared<TableScan>(table_wrapper_map.at("lineitem"), tpchq6_discount_predicate);
  first_scan->never_clear_output();
  first_scan->execute();
  const auto first_scan_result = first_scan->get_output();
  const auto second_scan = std::make_shared<TableScan>(first_scan, tpchq6_shipdate_less_predicate);
  second_scan->never_clear_output();
  second_scan->execute();

  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(second_scan, tpchq6_quantity_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TableScanIntegerOnPhysicalTable)(benchmark::State& state) {
  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper_map.at("lineitem"), int_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TableScanIntegerOnReferenceTable)(benchmark::State& state) {
  const auto table_scan = std::make_shared<TableScan>(table_wrapper_map.at("lineitem"), int_predicate);
  table_scan->never_clear_output();
  table_scan->execute();
  const auto scanned_table = table_scan->get_output();

  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    auto reference_table_scan = std::make_shared<TableScan>(table_scan, int_predicate);
    reference_table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TableScanStringOnPhysicalTable)(benchmark::State& state) {
  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper_map.at("lineitem"), string_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TableScanStringOnReferenceTable)(benchmark::State& state) {
  const auto table_scan = std::make_shared<TableScan>(table_wrapper_map.at("lineitem"), string_predicate);
  table_scan->never_clear_output();
  table_scan->execute();
  const auto scanned_table = table_scan->get_output();

  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    auto reference_table_scan = std::make_shared<TableScan>(table_scan, int_predicate);
    reference_table_scan->execute();
  }
}

/**
 * The objective of this benchmark is to measure performance improvements when having a sort-based aggregate on a
 * sorted column. This is not a TPC-H benchmark, it just uses TPC-H data (there are few joins on non-key columns in
 * TPC-H).
 */
BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_ScanAggregate)(benchmark::State& state) {
  // In this case, we use TPC-H lineitem table (largest table in dataset).
  // Assumption: We joined on shipmode, which is why we are sorted by that column
  // Aggregate: group by shipmode and count(l_orderkey_id)

  const auto& lineitem = table_wrapper_map.at("lineitem");
  const auto l_orderkey_id = ColumnID{0};
  const auto l_shipmode_id = ColumnID{10};

  const auto sorted_lineitem =
      std::make_shared<Sort>(lineitem, std::vector<SortColumnDefinition>{SortColumnDefinition{l_shipmode_id}});
  sorted_lineitem->never_clear_output();
  sorted_lineitem->execute();
  const auto mocked_table_scan_output = sorted_lineitem->get_output();
  const auto group_by_column = l_orderkey_id;
  const auto group_by = std::vector<ColumnID>{l_orderkey_id};
  const auto aggregate_expressions = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      count_(pqp_column_(group_by_column, mocked_table_scan_output->column_data_type(group_by_column),
                         mocked_table_scan_output->column_is_nullable(group_by_column),
                         mocked_table_scan_output->column_name(group_by_column)))};
  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    const auto aggregate = std::make_shared<AggregateSort>(sorted_lineitem, aggregate_expressions, group_by);
    aggregate->execute();
  }
}

/** TPC-H Q4 Benchmarks:
  - the following two benchmarks use a static and slightly simplified TPC-H Query 4
  - objective is to compare the performance of unnesting the EXISTS subquery

  - The LQPs translate roughly to this query:
      SELECT
         o_orderpriority
      FROM orders
      WHERE
         o_orderdate >= date '1993-07-01'
         AND o_orderdate < date '1993-10-01'
         AND exists (
             SELECT *
             FROM lineitem
             WHERE
                 l_orderkey = o_orderkey
                 AND l_commitdate < l_receiptdate
             )
 */
BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ4WithExistsSubquery)(benchmark::State& state) {
  // clang-format off
  const auto parameter = correlated_parameter_(ParameterID{0}, orders_orderkey);
  const auto subquery_lqp = PredicateNode::make(equals_(parameter, lineitem_orderkey),
      PredicateNode::make(less_than_(lineitem_commitdate, lineitem_receiptdate), lineitem_table_node));
  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, orders_orderkey));

  const auto lqp =
  ProjectionNode::make(expression_vector(orders_orderpriority),
    PredicateNode::make(equals_(exists_(subquery), 1),
      PredicateNode::make(greater_than_equals_(orders_orderdate, "1993-07-01"),
        PredicateNode::make(less_than_(orders_orderdate, "1993-10-01"),
         orders_table_node))));
  // clang-format on

  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    const auto pqp = LQPTranslator{}.translate_node(lqp);
    const auto& [tasks, root_operator_task] = OperatorTask::make_tasks_from_operator(pqp);
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ4WithUnnestedSemiJoin)(benchmark::State& state) {
  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(orders_orderpriority),
    JoinNode::make(JoinMode::Semi, equals_(lineitem_orderkey, orders_orderkey),
      PredicateNode::make(greater_than_equals_(orders_orderdate, "1993-07-01"),
        PredicateNode::make(less_than_(orders_orderdate, "1993-10-01"),
         orders_table_node)),
      PredicateNode::make(less_than_(lineitem_commitdate, lineitem_receiptdate), lineitem_table_node)));
  // clang-format on

  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    const auto pqp = LQPTranslator{}.translate_node(lqp);
    const auto& [tasks, root_operator_task] = OperatorTask::make_tasks_from_operator(pqp);
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  }
}

/**
 * For semi joins, the semi relation (which is filtered and returned in a semi join) is passed as the left input and
 * the other relation (which is solely checked for value existence and then discarded) is passed as the right side.
 *
 * For hash-based semi joins, inputs are switched as the left relation can probe the (later discarded) right relation.
 * In case the left relation is significantly smaller, the hash join does not perform optimally due to the switching.
 */
BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_HashSemiProbeRelationSmaller)(benchmark::State& state) {
  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    auto join = std::make_shared<JoinHash>(
        table_wrapper_map.at("orders"), table_wrapper_map.at("lineitem"), JoinMode::Semi,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_HashSemiProbeRelationLarger)(benchmark::State& state) {
  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    auto join = std::make_shared<JoinHash>(
        table_wrapper_map.at("lineitem"), table_wrapper_map.at("orders"), JoinMode::Semi,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
  }
}

BENCHMARK_DEFINE_F(TPCHDataMicroBenchmarkFixture, BM_LineitemHistogramCreation)(benchmark::State& state) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  const auto column_id = ColumnID{static_cast<ColumnID::base_type>(state.range(0))};

  const auto& storage_manager = Hyrise::get().storage_manager;
  const auto& lineitem_table = storage_manager.get_table("lineitem");

  const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, lineitem_table->row_count() / 2'000));

  const auto column_data_type = lineitem_table->column_data_type(column_id);

  resolve_data_type(column_data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
    for (auto _ : state) {
      EqualDistinctCountHistogram<ColumnDataType>::from_column(*lineitem_table, column_id, histogram_bin_count);
    }
  });
}

constexpr auto LINEITEM_COLUMN_COUNT = 15;
BENCHMARK_REGISTER_F(TPCHDataMicroBenchmarkFixture, BM_LineitemHistogramCreation)->DenseRange(0, LINEITEM_COLUMN_COUNT);

}  // namespace hyrise
