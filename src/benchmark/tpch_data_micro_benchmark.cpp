#include "micro_benchmark_basic_fixture.hpp"

#include "expression/expression_functional.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class TableWrapper;

// Defining the base fixture class
class TPCHDataMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) {
    auto& sm = StorageManager::get();
    const auto scale_factor = 0.001f;

    if (!sm.has_table("lineitem")) {
      std::cout << "Generating TPC-H data with scale factor " << scale_factor << " ... " << std::flush;
      TpchDbGenerator(scale_factor, 100'000).generate_and_store();
      std::cout << "done." << std::endl;

      // Tables are dictionary-encoded for the following benchmarks.
      const auto default_encoding = EncodingType::Dictionary;
      std::cout << "Encoding tables ... " << std::flush;
      ChunkEncoder::encode_all_chunks(sm.get_table("lineitem"),
                                      std::vector<SegmentEncodingSpec>(sm.get_table("lineitem")->column_count(),
                                                                       SegmentEncodingSpec(default_encoding)));
      ChunkEncoder::encode_all_chunks(sm.get_table("orders"),
                                      std::vector<SegmentEncodingSpec>(sm.get_table("orders")->column_count(),
                                                                       SegmentEncodingSpec(default_encoding)));
      std::cout << "done." << std::endl;
    }

    auto lineitem_tab = sm.get_table("lineitem");
    auto orders_tab = sm.get_table("orders");

    _lineitem_wrapper = std::make_shared<TableWrapper>(lineitem_tab);
    _lineitem_wrapper->execute();

    _orders_wrapper = std::make_shared<TableWrapper>(orders_tab);
    _orders_wrapper->execute();

    // TPC-H Q6 predicates. With an optimal predicate order (logical costs), discount (between on float) is first
    // executed, followed by shipdate <, followed by quantity, and eventually shipdate >= (note, order calculated
    // assuming non-inclusive between predicates are not yet supported).
    // This order is not necessarily the order Hyrise uses (estimates can be vastly off) or which will eventually
    // be calculated by more sophisticated cost models.
    _tpchq6_discount_operand = pqp_column_(ColumnID{6}, lineitem_tab->column_data_type(ColumnID{6}),
                                           lineitem_tab->column_is_nullable(ColumnID{6}), "");
    _tpchq6_discount_predicate =
        std::make_shared<BetweenExpression>(_tpchq6_discount_operand, value_(0.05), value_(0.70001));

    _tpchq6_shipdate_less_operand = pqp_column_(ColumnID{10}, lineitem_tab->column_data_type(ColumnID{10}),
                                                lineitem_tab->column_is_nullable(ColumnID{10}), "");
    _tpchq6_shipdate_less_predicate = std::make_shared<BinaryPredicateExpression>(
        PredicateCondition::LessThan, _tpchq6_shipdate_less_operand, value_("1995-01-01"));

    _tpchq6_quantity_operand = pqp_column_(ColumnID{4}, lineitem_tab->column_data_type(ColumnID{4}),
                                           lineitem_tab->column_is_nullable(ColumnID{4}), "");
    _tpchq6_quantity_predicate =
        std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, _tpchq6_quantity_operand, value_(24));

    // The following two "synthetic" predicates have a selectivity of 1.0
    _lorderkey_operand = pqp_column_(ColumnID{0}, lineitem_tab->column_data_type(ColumnID{0}),
                                     lineitem_tab->column_is_nullable(ColumnID{0}), "");
    _int_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals,
                                                                 _lorderkey_operand, value_(-5));

    _lshipinstruct_operand = pqp_column_(ColumnID{13}, lineitem_tab->column_data_type(ColumnID{13}),
                                         lineitem_tab->column_is_nullable(ColumnID{13}), "");
    _string_predicate =
        std::make_shared<BinaryPredicateExpression>(PredicateCondition::NotEquals, _lshipinstruct_operand, value_("a"));
  }

  void TearDown(::benchmark::State&) {}

  inline static bool _tpch_data_generated = false;

  std::shared_ptr<TableWrapper> _lineitem_wrapper;
  std::shared_ptr<TableWrapper> _orders_wrapper;
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
};

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ6FirstScanPredicate)(benchmark::State& state) {
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(_lineitem_wrapper, _tpchq6_discount_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ6SecondScanPredicate)(benchmark::State& state) {
  const auto first_scan = std::make_shared<TableScan>(_lineitem_wrapper, _tpchq6_discount_predicate);
  first_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(first_scan, _tpchq6_shipdate_less_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ6ThirdScanPredicate)(benchmark::State& state) {
  const auto first_scan = std::make_shared<TableScan>(_lineitem_wrapper, _tpchq6_discount_predicate);
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
    const auto table_scan = std::make_shared<TableScan>(_lineitem_wrapper, _int_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TableScanIntegerOnReferenceTable)(benchmark::State& state) {
  const auto table_scan = std::make_shared<TableScan>(_lineitem_wrapper, _int_predicate);
  table_scan->execute();
  const auto scanned_table = table_scan->get_output();

  for (auto _ : state) {
    auto reference_table_scan = std::make_shared<TableScan>(table_scan, _int_predicate);
    reference_table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TableScanStringOnPhysicalTable)(benchmark::State& state) {
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(_lineitem_wrapper, _string_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TableScanStringOnReferenceTable)(benchmark::State& state) {
  const auto table_scan = std::make_shared<TableScan>(_lineitem_wrapper, _string_predicate);
  table_scan->execute();
  const auto scanned_table = table_scan->get_output();

  for (auto _ : state) {
    auto reference_table_scan = std::make_shared<TableScan>(table_scan, _int_predicate);
    reference_table_scan->execute();
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
    auto join = std::make_shared<JoinHash>(_orders_wrapper, _lineitem_wrapper, JoinMode::Semi,
                                           ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    join->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_HashSemiProbeRelationLarger)(benchmark::State& state) {
  for (auto _ : state) {
    auto join = std::make_shared<JoinHash>(_lineitem_wrapper, _orders_wrapper, JoinMode::Semi,
                                           ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    join->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_SortMergeSemiProbeRelationSmaller)(benchmark::State& state) {
  for (auto _ : state) {
    auto join = std::make_shared<JoinSortMerge>(_orders_wrapper, _lineitem_wrapper, JoinMode::Semi,
                                                ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    join->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_SortMergeSemiProbeRelationLarger)(benchmark::State& state) {
  for (auto _ : state) {
    auto join = std::make_shared<JoinSortMerge>(_lineitem_wrapper, _orders_wrapper, JoinMode::Semi,
                                                ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    join->execute();
  }
}

}  // namespace opossum
