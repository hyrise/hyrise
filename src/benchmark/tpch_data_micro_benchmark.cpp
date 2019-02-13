#include "micro_benchmark_basic_fixture.hpp"

#include "benchmark_config.hpp"
#include "constant_mappings.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_table_generator.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class TableWrapper;

// Defining the base fixture class
class TPCHDataMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) {
    auto& sm = StorageManager::get();
    const auto scale_factor = 0.001f;
    const auto default_encoding = EncodingType::Dictionary;

    auto benchmark_config = BenchmarkConfig::get_default_config();
    // TODO(anyone): setup benchmark_config with the given default_encoding
    // benchmark_config.encoding_config = EncodingConfig{SegmentEncodingSpec{default_encoding}};

    if (!sm.has_table("lineitem")) {
      std::cout << "Generating TPC-H data set with scale factor " << scale_factor << " and "
                << encoding_type_to_string.left.at(default_encoding) << " encoding:" << std::endl;
      TpchTableGenerator(scale_factor, std::make_shared<BenchmarkConfig>(benchmark_config)).generate_and_store();
    }

    _table_wrapper_map = create_table_wrappers(sm);

    auto lineitem_table = sm.get_table("lineitem");

    // TPC-H Q6 predicates. With an optimal predicate order (logical costs), discount (between on float) is first
    // executed, followed by shipdate <, followed by quantity, and eventually shipdate >= (note, order calculated
    // assuming non-inclusive between predicates are not yet supported).
    // This order is not necessarily the order Hyrise uses (estimates can be vastly off) or which will eventually
    // be calculated by more sophisticated cost models.
    _tpchq6_discount_operand = pqp_column_(ColumnID{6}, lineitem_table->column_data_type(ColumnID{6}),
                                           lineitem_table->column_is_nullable(ColumnID{6}), "");
    _tpchq6_discount_predicate =
        std::make_shared<BetweenExpression>(_tpchq6_discount_operand, value_(0.05), value_(0.70001));

    _tpchq6_shipdate_less_operand = pqp_column_(ColumnID{10}, lineitem_table->column_data_type(ColumnID{10}),
                                                lineitem_table->column_is_nullable(ColumnID{10}), "");
    _tpchq6_shipdate_less_predicate = std::make_shared<BinaryPredicateExpression>(
        PredicateCondition::LessThan, _tpchq6_shipdate_less_operand, value_("1995-01-01"));

    _tpchq6_quantity_operand = pqp_column_(ColumnID{4}, lineitem_table->column_data_type(ColumnID{4}),
                                           lineitem_table->column_is_nullable(ColumnID{4}), "");
    _tpchq6_quantity_predicate =
        std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, _tpchq6_quantity_operand, value_(24));

    // The following two "synthetic" predicates have a selectivity of 1.0
    _lorderkey_operand = pqp_column_(ColumnID{0}, lineitem_table->column_data_type(ColumnID{0}),
                                     lineitem_table->column_is_nullable(ColumnID{0}), "");
    _int_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals,
                                                                 _lorderkey_operand, value_(-5));

    _lshipinstruct_operand = pqp_column_(ColumnID{13}, lineitem_table->column_data_type(ColumnID{13}),
                                         lineitem_table->column_is_nullable(ColumnID{13}), "");
    _string_predicate =
        std::make_shared<BinaryPredicateExpression>(PredicateCondition::NotEquals, _lshipinstruct_operand, value_("a"));

    _orders_table_node = StoredTableNode::make("orders");
    _orders_orderpriority = _orders_table_node->get_column("o_orderpriority");
    _orders_orderdate = _orders_table_node->get_column("o_orderdate");
    _orders_orderkey = _orders_table_node->get_column("o_orderkey");

    _lineitem_table_node = StoredTableNode::make("lineitem");
    _lineitem_orderkey = _lineitem_table_node->get_column("l_orderkey");
    _lineitem_commitdate = _lineitem_table_node->get_column("l_commitdate");
    _lineitem_receiptdate = _lineitem_table_node->get_column("l_receiptdate");
  }

  // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
  void TearDown(::benchmark::State&) {}

  std::map<std::string, std::shared_ptr<TableWrapper>> create_table_wrappers(StorageManager& sm) {
    std::map<std::string, std::shared_ptr<TableWrapper>> wrapper_map;
    for (const auto& table_name : sm.table_names()) {
      auto table = sm.get_table(table_name);
      auto table_wrapper = std::make_shared<TableWrapper>(table);
      table_wrapper->execute();

      wrapper_map.emplace(table_name, table_wrapper);
    }

    return wrapper_map;
  }

  inline static bool _tpch_data_generated = false;

  std::map<std::string, std::shared_ptr<TableWrapper>> _table_wrapper_map;

  std::shared_ptr<PQPColumnExpression> _lorderkey_operand;
  std::shared_ptr<BinaryPredicateExpression> _int_predicate;
  std::shared_ptr<PQPColumnExpression> _lshipinstruct_operand;
  std::shared_ptr<BinaryPredicateExpression> _string_predicate;

  std::shared_ptr<PQPColumnExpression> _tpchq6_discount_operand;
  std::shared_ptr<BetweenExpression> _tpchq6_discount_predicate;
  std::shared_ptr<PQPColumnExpression> _tpchq6_shipdate_less_operand;
  std::shared_ptr<BinaryPredicateExpression> _tpchq6_shipdate_less_predicate;
  std::shared_ptr<PQPColumnExpression> _tpchq6_quantity_operand;
  std::shared_ptr<BinaryPredicateExpression> _tpchq6_quantity_predicate;

  std::shared_ptr<StoredTableNode> _orders_table_node, _lineitem_table_node;
  LQPColumnReference _orders_orderpriority, _orders_orderdate, _orders_orderkey;
  LQPColumnReference _lineitem_orderkey, _lineitem_commitdate, _lineitem_receiptdate;
};

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ6FirstScanPredicate)(benchmark::State& state) {
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _tpchq6_discount_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ6SecondScanPredicate)(benchmark::State& state) {
  const auto first_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _tpchq6_discount_predicate);
  first_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(first_scan, _tpchq6_shipdate_less_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ6ThirdScanPredicate)(benchmark::State& state) {
  const auto first_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _tpchq6_discount_predicate);
  first_scan->execute();
  const auto first_scan_result = first_scan->get_output();
  const auto second_scan = std::make_shared<TableScan>(first_scan, _tpchq6_shipdate_less_predicate);
  second_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(second_scan, _tpchq6_quantity_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TableScanIntegerOnPhysicalTable)(benchmark::State& state) {
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _int_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TableScanIntegerOnReferenceTable)(benchmark::State& state) {
  const auto table_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _int_predicate);
  table_scan->execute();
  const auto scanned_table = table_scan->get_output();

  for (auto _ : state) {
    auto reference_table_scan = std::make_shared<TableScan>(table_scan, _int_predicate);
    reference_table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TableScanStringOnPhysicalTable)(benchmark::State& state) {
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _string_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TableScanStringOnReferenceTable)(benchmark::State& state) {
  const auto table_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _string_predicate);
  table_scan->execute();
  const auto scanned_table = table_scan->get_output();

  for (auto _ : state) {
    auto reference_table_scan = std::make_shared<TableScan>(table_scan, _int_predicate);
    reference_table_scan->execute();
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
  const auto parameter = correlated_parameter_(ParameterID{0}, _orders_orderkey);
  const auto subquery_lqp = PredicateNode::make(equals_(parameter, _lineitem_orderkey),
      PredicateNode::make(less_than_(_lineitem_commitdate, _lineitem_receiptdate), _lineitem_table_node));
  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, _orders_orderkey));

  const auto lqp =
  ProjectionNode::make(expression_vector(_orders_orderpriority),
    PredicateNode::make(equals_(exists_(subquery), 1),
      PredicateNode::make(greater_than_equals_(_orders_orderdate, "1993-07-01"),
        PredicateNode::make(less_than_(_orders_orderdate, "1993-10-01"),
         _orders_table_node))));
  // clang-format on

  for (auto _ : state) {
    const auto pqp = LQPTranslator{}.translate_node(lqp);
    const auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::Yes);
    CurrentScheduler::schedule_and_wait_for_tasks(tasks);
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ4WithUnnestedSemiJoin)(benchmark::State& state) {
  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(_orders_orderpriority),
    JoinNode::make(JoinMode::Semi, equals_(_lineitem_orderkey, _orders_orderkey),
      PredicateNode::make(greater_than_equals_(_orders_orderdate, "1993-07-01"),
        PredicateNode::make(less_than_(_orders_orderdate, "1993-10-01"),
         _orders_table_node)),
      PredicateNode::make(less_than_(_lineitem_commitdate, _lineitem_receiptdate), _lineitem_table_node)));
  // clang-format on

  for (auto _ : state) {
    const auto pqp = LQPTranslator{}.translate_node(lqp);
    const auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::Yes);
    CurrentScheduler::schedule_and_wait_for_tasks(tasks);
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
  for (auto _ : state) {
    auto join =
        std::make_shared<JoinHash>(_table_wrapper_map.at("orders"), _table_wrapper_map.at("lineitem"), JoinMode::Semi,
                                   ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    join->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_HashSemiProbeRelationLarger)(benchmark::State& state) {
  for (auto _ : state) {
    auto join =
        std::make_shared<JoinHash>(_table_wrapper_map.at("lineitem"), _table_wrapper_map.at("orders"), JoinMode::Semi,
                                   ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    join->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_SortMergeSemiProbeRelationSmaller)(benchmark::State& state) {
  for (auto _ : state) {
    auto join = std::make_shared<JoinSortMerge>(_table_wrapper_map.at("orders"), _table_wrapper_map.at("lineitem"),
                                                JoinMode::Semi, ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                                PredicateCondition::Equals);
    join->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_SortMergeSemiProbeRelationLarger)(benchmark::State& state) {
  for (auto _ : state) {
    auto join = std::make_shared<JoinSortMerge>(_table_wrapper_map.at("lineitem"), _table_wrapper_map.at("orders"),
                                                JoinMode::Semi, ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                                PredicateCondition::Equals);
    join->execute();
  }
}

}  // namespace opossum
