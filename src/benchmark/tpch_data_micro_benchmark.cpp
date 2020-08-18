#include <chrono>
#include <iostream>
#include <fstream>

#include "micro_benchmark_basic_fixture.hpp"

#include "benchmark_config.hpp"
#include "constant_mappings.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/join_hash.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "storage/encoding_type.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

using duration_unit = std::chrono::microseconds;

class TableWrapper;

// Defining the base fixture class
class TPCHDataMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) {
    auto& sm = Hyrise::get().storage_manager;
    const auto scale_factor = 0.1f;
    const auto default_encoding = EncodingType::Dictionary;

    auto benchmark_config = BenchmarkConfig::get_default_config();
    // TODO(anyone): setup benchmark_config with the given default_encoding
    // benchmark_config.encoding_config = EncodingConfig{SegmentEncodingSpec{default_encoding}};

    if (!sm.has_table("lineitem")) {
      std::cout << "Generating TPC-H data set with scale factor " << scale_factor << " and " << default_encoding
                << " encoding:" << std::endl;
      TPCHTableGenerator(scale_factor, std::make_shared<BenchmarkConfig>(benchmark_config)).generate_and_store();
    }

    _table_wrapper_map = create_table_wrappers(sm);

    auto lineitem_table = sm.get_table("lineitem");

    std::ofstream compression_selection_selectivities;
    compression_selection_selectivities.open("compression_selection_filters.csv");
    compression_selection_selectivities << "PREDICATE_COLUMN,TABLE_LENGTH,DISTINCT_ELEMENTS,SELECTIVITY\n";

    const auto row_count_lineitem = lineitem_table->row_count();

    auto ll_sql_pipeline = SQLPipelineBuilder{"SELECT l_linenumber, count(*) FROM lineitem group by l_linenumber order by count(*) asc"}.create_pipeline();
    const auto [ll_pipeline_status, ll_table] = ll_sql_pipeline.get_result_table();
    const auto search_value_l_linenumber = ll_table->get_value<int>(ColumnID{0}, 0u);
    const auto count_l_linenumber = ll_table->get_value<int64_t>(ColumnID{1}, 0u);
    std::cout << "Searching l_linenumber for »" << *search_value_l_linenumber << "<<." << std::endl;
    compression_selection_selectivities << "L_LINENUMBER," << row_count_lineitem << "," << ll_table->row_count() << "," << static_cast<double>(*count_l_linenumber) / static_cast<double>(row_count_lineitem) << "\n";

    auto lp_sql_pipeline = SQLPipelineBuilder{"SELECT l_partkey, count(*) FROM lineitem group by l_partkey order by count(*) desc"}.create_pipeline();
    const auto [lp_pipeline_status, lp_table] = lp_sql_pipeline.get_result_table();
    const auto search_value_l_partkey = lp_table->get_value<int>(ColumnID{0}, 0u);
    const auto count_l_partkey = lp_table->get_value<int64_t>(ColumnID{1}, 0u);
    std::cout << "Searching l_partkey for »" << *search_value_l_partkey << "<<." << std::endl;
    compression_selection_selectivities << "L_PARTKEY," << row_count_lineitem << "," << lp_table->row_count() << "," << static_cast<double>(*count_l_partkey) / static_cast<double>(row_count_lineitem) << "\n";

    auto lc_sql_pipeline = SQLPipelineBuilder{"SELECT l_comment, count(*) FROM lineitem group by l_comment order by count(*) desc"}.create_pipeline();
    const auto [lc_pipeline_status, lc_table] = lc_sql_pipeline.get_result_table();
    const auto search_value_l_comment = lc_table->get_value<pmr_string>(ColumnID{0}, 0u);
    const auto count_l_comment = lc_table->get_value<int64_t>(ColumnID{1}, 0u);
    std::cout << "Searching l_comment for »" << *search_value_l_comment << "<<." << std::endl;
    compression_selection_selectivities << "L_COMMENT," << row_count_lineitem << "," << lc_table->row_count() <<  "," << static_cast<double>(*count_l_comment) / static_cast<double>(row_count_lineitem) << "\n";

    compression_selection_selectivities.close();

    // Predicates as in TPC-H Q6, ordered by selectivity. Not necessarily the same order as determined by the optimizer
    _tpchq6_discount_operand = pqp_column_(ColumnID{6}, lineitem_table->column_data_type(ColumnID{6}),
                                           lineitem_table->column_is_nullable(ColumnID{6}), "");
    _tpchq6_discount_predicate = std::make_shared<BetweenExpression>(
        PredicateCondition::BetweenInclusive, _tpchq6_discount_operand, value_(0.05), value_(0.70001));

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


    _l_linenumber_operand = pqp_column_(ColumnID{3}, lineitem_table->column_data_type(ColumnID{3}),
                                                lineitem_table->column_is_nullable(ColumnID{3}), "");
    _l_linenumber_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, _l_linenumber_operand,
                                                                          value_(*search_value_l_linenumber));

    _l_partkey_operand = pqp_column_(ColumnID{1}, lineitem_table->column_data_type(ColumnID{1}),
                                                lineitem_table->column_is_nullable(ColumnID{1}), "");
    _l_partkey_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, _l_partkey_operand,
                                                                          value_(*search_value_l_partkey));

    _last_column_id_lineitem = static_cast<uint16_t>(lineitem_table->column_count()) - 1;
    _l_comment_operand = pqp_column_(ColumnID{_last_column_id_lineitem}, lineitem_table->column_data_type(ColumnID{_last_column_id_lineitem}),
                                                lineitem_table->column_is_nullable(ColumnID{_last_column_id_lineitem}), "");
    _l_comment_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, _l_comment_operand,
                                                                       value_(*search_value_l_comment));

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
  inline static bool _compression_benchmark_done = false;

  uint16_t _last_column_id_lineitem;

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

  std::shared_ptr<PQPColumnExpression> _l_linenumber_operand;
  std::shared_ptr<BinaryPredicateExpression> _l_linenumber_predicate;
  std::shared_ptr<PQPColumnExpression> _l_partkey_operand;
  std::shared_ptr<BinaryPredicateExpression> _l_partkey_predicate;
  std::shared_ptr<PQPColumnExpression> _l_comment_operand;
  std::shared_ptr<BinaryPredicateExpression> _l_comment_predicate;

  std::shared_ptr<StoredTableNode> _orders_table_node, _lineitem_table_node;
  std::shared_ptr<LQPColumnExpression> _orders_orderpriority, _orders_orderdate, _orders_orderkey;
  std::shared_ptr<LQPColumnExpression> _lineitem_orderkey, _lineitem_commitdate, _lineitem_receiptdate;
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

/**
 * The objective of this benchmark is to measure performance improvements when having a sort-based aggregate on a
 * sorted column. This is not a TPC-H benchmark, it just uses TPC-H data (there are few joins on non-key columns in
 * TPC-H).
 */
BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_ScanAggregate)(benchmark::State& state) {
  // In this case, we use TPC-H lineitem table (largest table in dataset).
  // Assumption: We joined on shipmode, which is why we are sorted by that column
  // Aggregate: group by shipmode and count(l_orderkey_id)

  const auto& lineitem = _table_wrapper_map.at("lineitem");
  const auto l_orderkey_id = ColumnID{0};
  const auto l_shipmode_id = ColumnID{10};

  const auto sorted_lineitem =
      std::make_shared<Sort>(lineitem, std::vector<SortColumnDefinition>{SortColumnDefinition{l_shipmode_id}});
  sorted_lineitem->execute();
  const auto mocked_table_scan_output = sorted_lineitem->get_output();
  const ColumnID group_by_column = l_orderkey_id;
  const std::vector<ColumnID> group_by = {l_orderkey_id};
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{
      count_(pqp_column_(group_by_column, mocked_table_scan_output->column_data_type(group_by_column),
                         mocked_table_scan_output->column_is_nullable(group_by_column),
                         mocked_table_scan_output->column_name(group_by_column)))};
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
    const auto tasks = OperatorTask::make_tasks_from_operator(pqp);
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
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
    const auto tasks = OperatorTask::make_tasks_from_operator(pqp);
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
  for (auto _ : state) {
    auto join = std::make_shared<JoinHash>(
        _table_wrapper_map.at("orders"), _table_wrapper_map.at("lineitem"), JoinMode::Semi,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_HashSemiProbeRelationLarger)(benchmark::State& state) {
  for (auto _ : state) {
    auto join = std::make_shared<JoinHash>(
        _table_wrapper_map.at("lineitem"), _table_wrapper_map.at("orders"), JoinMode::Semi,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();
  }
}

std::string segment_encoding_to_csv_string(const SegmentEncodingSpec& seg_spec) {
  std::stringstream result;

  result << encoding_type_to_string.left.at(seg_spec.encoding_type) << ",";
  if (seg_spec.vector_compression_type) {
    result << vector_compression_type_to_string.left.at(*seg_spec.vector_compression_type);
  } else {
    result << "None";
  }

  return result.str();
}

template <typename D>
std::stringstream csv_time_measurement_row(const std::string&& benchmark_name, const SegmentEncodingSpec& seg_spec,
  const size_t repetitions, const D begin, const D end, const size_t nth_of_a_second, std::ofstream& output_file) {
  std::stringstream result;

  const auto duration = std::chrono::duration_cast<duration_unit>(end - begin); 

  result << benchmark_name << "," << segment_encoding_to_csv_string(seg_spec) << ",";
  if (begin == end) {
    // in case nanosecond timings equal, something went wrong
    result << repetitions << ",NULL," << nth_of_a_second << "\n";
  } else {
    result << repetitions << "," << duration.count() << "," << nth_of_a_second << "\n";
  }

  output_file << result.str();

  return result;
}

std::stringstream csv_size_measurement_row(const std::string&& benchmark_name, const SegmentEncodingSpec& seg_spec,
    const size_t size, std::ofstream& output_file) {
  std::stringstream result;
  result << benchmark_name << "," << segment_encoding_to_csv_string(seg_spec) << ",";
  if (size == 0) {
    // Something went wrong, when data disappeared
    result << "NULL\n";
  } else {
    result << size << "\n";
  }
  output_file << result.str();

  return result;
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_CompressionSelectionIntroTable)(benchmark::State& state) {
  if (_compression_benchmark_done) {
    return;
  }

  auto& sm = Hyrise::get().storage_manager;
  auto table = sm.get_table("lineitem");

  ChunkEncodingSpec chunk_spec{table->column_count(), SegmentEncodingSpec{EncodingType::Dictionary}};

  const auto all_test_segment_encodings = {SegmentEncodingSpec{EncodingType::Unencoded},
                                           SegmentEncodingSpec{EncodingType::LZ4},
                                           SegmentEncodingSpec{EncodingType::RunLength},
                                           SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned},
                                           SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::SimdBp128},
                                           SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::FixedSizeByteAligned},
                                           SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::SimdBp128}};

  // constexpr size_t encoding_measurements = 100;
  // constexpr size_t filter_measurements = 1'000;

  constexpr size_t encoding_measurements = 5;
  constexpr size_t filter_measurements = 10;

  constexpr size_t nth_of_a_second = 1'000'000;

  std::ofstream runtime_measurements;
  runtime_measurements.open("compression_selection_runtime_measurements.csv");
  runtime_measurements << "Benchmark,Encoding,VectorCompression,Repetitions,Runtime,NthOfASecond\n";

  std::ofstream size_measurements;
  size_measurements.open("compression_selection_size_measurements.csv");
  size_measurements << "Benchmark,Encoding,VectorCompression,Size\n";

  for (auto seg_spec : all_test_segment_encodings) {
    chunk_spec[1] = seg_spec;
    chunk_spec[3] = seg_spec;
    if (seg_spec.encoding_type != EncodingType::FrameOfReference) {
      chunk_spec.back() = seg_spec;
    }
    ChunkEncoder::encode_all_chunks(table, chunk_spec);


    for (auto i = size_t{0}; i < encoding_measurements; ++i) {
      if (seg_spec.encoding_type == EncodingType::Unencoded) {
        {
          const auto start = std::chrono::high_resolution_clock::now();
          csv_time_measurement_row("UnencodingPartkey", seg_spec, encoding_measurements, start, start, nth_of_a_second, runtime_measurements).str();
          csv_time_measurement_row("EncodingPartkey", seg_spec, encoding_measurements, start, start, nth_of_a_second, runtime_measurements).str();
          csv_time_measurement_row("UnencodingComment", seg_spec, encoding_measurements, start, start, nth_of_a_second, runtime_measurements).str();
          csv_time_measurement_row("EncodingComment", seg_spec, encoding_measurements, start, start, nth_of_a_second, runtime_measurements).str();
          break;
        }
      }
      /**
          PARTKEY
       */
      chunk_spec[1] = SegmentEncodingSpec{EncodingType::Unencoded};
      {
        const auto start = std::chrono::high_resolution_clock::now();
        ChunkEncoder::encode_all_chunks(table, chunk_spec);
        const auto end = std::chrono::high_resolution_clock::now();
        csv_time_measurement_row("UnencodingPartkey", seg_spec, encoding_measurements, start, end, nth_of_a_second, runtime_measurements).str();
      }
      chunk_spec[1] = seg_spec;
      {
        const auto start = std::chrono::high_resolution_clock::now();
        ChunkEncoder::encode_all_chunks(table, chunk_spec);
        const auto end = std::chrono::high_resolution_clock::now();
        csv_time_measurement_row("EncodingPartkey", seg_spec, encoding_measurements, start, end, nth_of_a_second, runtime_measurements).str();
      }

      /**
          COMMENT
       */
      if (seg_spec.encoding_type != EncodingType::FrameOfReference) {
        chunk_spec.back() = SegmentEncodingSpec{EncodingType::Unencoded};
        {
          const auto start = std::chrono::high_resolution_clock::now();
          ChunkEncoder::encode_all_chunks(table, chunk_spec);
          const auto end = std::chrono::high_resolution_clock::now();
          csv_time_measurement_row("UnencodingComment", seg_spec, encoding_measurements, start, end, nth_of_a_second, runtime_measurements).str();
        }

        chunk_spec.back() = seg_spec;
        {
          const auto start = std::chrono::high_resolution_clock::now();
          ChunkEncoder::encode_all_chunks(table, chunk_spec);
          const auto end = std::chrono::high_resolution_clock::now();
          csv_time_measurement_row("EncodingComment", seg_spec, encoding_measurements, start, end, nth_of_a_second, runtime_measurements).str();
        }
      } else {
        {
          const auto start = std::chrono::high_resolution_clock::now();
          csv_time_measurement_row("UnencodingComment", seg_spec, encoding_measurements, start, start, nth_of_a_second, runtime_measurements).str();
          csv_time_measurement_row("EncodingComment", seg_spec, encoding_measurements, start, start, nth_of_a_second, runtime_measurements).str();
        }
      }
    }

    // if (seg_spec.encoding_type != EncodingType::LZ4) {
    //   std::stringstream validation_output;
    //   Print::print(table, PrintFlags::None, validation_output);
    //   std::cout << validation_output.str().substr(0, 1000) << std::endl;
    // }

    for (auto i = size_t{0}; i < filter_measurements; ++i) {
      const auto table_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _l_linenumber_predicate);

      {
        auto start = std::chrono::high_resolution_clock::now();
        table_scan->execute();
        auto end = std::chrono::high_resolution_clock::now();
        // std::cout << "lines:" << table_scan->get_output()->row_count() << std::endl;

        csv_time_measurement_row("DirectScan_l_linenumber", seg_spec, filter_measurements, start, end, nth_of_a_second, runtime_measurements).str();
      }
    }

    for (auto i = size_t{0}; i < filter_measurements; ++i) {
      const auto table_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _l_partkey_predicate);

      {
        auto start = std::chrono::high_resolution_clock::now();
        table_scan->execute();
        auto end = std::chrono::high_resolution_clock::now();

        csv_time_measurement_row("DirectScan_l_partkey", seg_spec, filter_measurements, start, end, nth_of_a_second, runtime_measurements).str();
      }
    }

    for (auto i = size_t{0}; i < filter_measurements; ++i) {
      const auto table_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _l_comment_predicate);

      if (seg_spec.encoding_type != EncodingType::FrameOfReference) {
        {
          auto start = std::chrono::high_resolution_clock::now();
          table_scan->execute();
          auto end = std::chrono::high_resolution_clock::now();

          csv_time_measurement_row("DirectScan_l_comment", seg_spec, filter_measurements, start, end, nth_of_a_second, runtime_measurements).str();
        }
      } else {
        {
          auto start = std::chrono::high_resolution_clock::now();
          csv_time_measurement_row("DirectScan_l_comment", seg_spec, filter_measurements, start, start, nth_of_a_second, runtime_measurements).str();
        }
      }
    }

    for (auto i = size_t{0}; i < filter_measurements; ++i) {
      const auto table_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _l_linenumber_predicate);
      table_scan->execute();
      const auto postfilter_scan = std::make_shared<TableScan>(table_scan, _l_partkey_predicate);

      {
        auto start = std::chrono::high_resolution_clock::now();
        postfilter_scan->execute();
        auto end = std::chrono::high_resolution_clock::now();
        // std::cout << "lines:" << table_scan->get_output()->row_count() << std::endl;

        csv_time_measurement_row("PostFilter_l_linenumber", seg_spec, filter_measurements, start, end, nth_of_a_second, runtime_measurements).str();
      }
    }

    auto column_size_partkey = size_t{0};
    auto column_size_linenumber = size_t{0};
    auto column_size_comment = size_t{0};
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      column_size_partkey += chunk->get_segment(ColumnID{1})->memory_usage(MemoryUsageCalculationMode::Full);
      column_size_linenumber += chunk->get_segment(ColumnID{3})->memory_usage(MemoryUsageCalculationMode::Full);
      column_size_comment += chunk->get_segment(ColumnID{_last_column_id_lineitem})->memory_usage(MemoryUsageCalculationMode::Full);
    }

    csv_size_measurement_row("Partkey", seg_spec, column_size_partkey, size_measurements);
    csv_size_measurement_row("Linenumber", seg_spec, column_size_linenumber, size_measurements);
    if (seg_spec.encoding_type != EncodingType::FrameOfReference) {
      csv_size_measurement_row("Comment", seg_spec, column_size_comment, size_measurements);
    } else {
      csv_size_measurement_row("Comment", seg_spec, 0, size_measurements);
    }

    runtime_measurements.flush();
    size_measurements.flush();
  }
  runtime_measurements.close();
  size_measurements.close();

  _compression_benchmark_done = true;
}

}  // namespace opossum
