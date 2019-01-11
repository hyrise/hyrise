#include "micro_benchmark_basic_fixture.hpp"

#include "tpch/tpch_db_generator.hpp"
#include "storage/storage_manager.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "operators/table_scan.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_wrapper.hpp"
#include "expression/expression_functional.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class TableWrapper;

// Defining the base fixture class
class TPCHDataMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) {
  	auto& sm = StorageManager::get();

  	if (!sm.has_table("lineitem")) {
  		std::cout << "Generating TPC-H data ... " << std::flush;
    	TpchDbGenerator(0.001f, 100'000).generate_and_store();
    	std::cout << "done." << std::endl;

    	const auto default_encoding = EncodingType::Dictionary;
    	std::cout << "Encoding tables ... " << std::flush;
    	ChunkEncoder::encode_all_chunks(sm.get_table("lineitem"), std::vector<SegmentEncodingSpec>(sm.get_table("lineitem")->column_count(), SegmentEncodingSpec(default_encoding)));
    	ChunkEncoder::encode_all_chunks(sm.get_table("orders"), std::vector<SegmentEncodingSpec>(sm.get_table("orders")->column_count(), SegmentEncodingSpec(default_encoding)));
    	std::cout << "done." << std::endl;
  	}

    auto lineitem_tab = sm.get_table("lineitem");
    auto orders_tab = sm.get_table("orders");

    _lineitem_wrapper = std::make_shared<TableWrapper>(lineitem_tab);
    _lineitem_wrapper->execute();

    _orders_wrapper = std::make_shared<TableWrapper>(orders_tab);
    _orders_wrapper->execute();

    _lorderkey_operand = pqp_column_(ColumnID{0}, lineitem_tab->column_data_type(ColumnID{0}), lineitem_tab->column_is_nullable(ColumnID{0}), "");
  	_int_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, _lorderkey_operand, value_(-5));

  	_lshipinstruct_operand = pqp_column_(ColumnID{13}, lineitem_tab->column_data_type(ColumnID{13}), lineitem_tab->column_is_nullable(ColumnID{13}), "");
  	_string_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::NotEquals, _lshipinstruct_operand, value_("a"));
  }

  void TearDown(::benchmark::State&) {
  }

  inline static bool _tpch_data_generated = false;

  std::shared_ptr<TableWrapper> _lineitem_wrapper;
  std::shared_ptr<TableWrapper> _orders_wrapper;
  std::shared_ptr<PQPColumnExpression> _lorderkey_operand;
  std::shared_ptr<BinaryPredicateExpression> _int_predicate;
  std::shared_ptr<PQPColumnExpression> _lshipinstruct_operand;
  std::shared_ptr<BinaryPredicateExpression> _string_predicate;
};

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

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_HashSemiProbeSmaller)(benchmark::State& state) {
  for (auto _ : state) {
    auto join = std::make_shared<JoinHash>(_lineitem_wrapper, _orders_wrapper, JoinMode::Semi, ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    join->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_HashSemiProbeLarger)(benchmark::State& state) {
  for (auto _ : state) {
    auto join = std::make_shared<JoinHash>(_orders_wrapper, _lineitem_wrapper, JoinMode::Semi, ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    join->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_SortMergeSemiProbeSmaller)(benchmark::State& state) {
  for (auto _ : state) {
    auto join = std::make_shared<JoinSortMerge>(_lineitem_wrapper, _orders_wrapper, JoinMode::Semi, ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    join->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_SortMergeSemiProbeLarger)(benchmark::State& state) {
  for (auto _ : state) {
    auto join = std::make_shared<JoinSortMerge>(_orders_wrapper, _lineitem_wrapper, JoinMode::Semi, ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
    join->execute();
  }
}

}  // namespace opossum


