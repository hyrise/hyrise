#include <chrono>
#include <iostream>
#include <fstream>
#include <random>

#include "micro_benchmark_basic_fixture.hpp"

#include "benchmark_config.hpp"
#include "constant_mappings.hpp"
#include "storage/encoding_type.hpp"
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
#include "operators/print.hpp"
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

constexpr auto SELECTIVITY = 0.1f;

const auto LINEITEM_COLUMNS_TO_PROCESS = std::map<std::string, std::pair<ColumnID, DataType>>{
  {"l_partkey", {ColumnID{1}, DataType::Int}},
  {"l_linenumber", {ColumnID{3}, DataType::Int}},
  {"l_shipmode", {ColumnID{14}, DataType::String}},
  {"l_comment", {ColumnID{15}, DataType::String}}
};

/**
 * We are walking the results backwards (desc) to obtain the small values of l_partkey. Otherwise, we start witht he largest value
 * which would yield a selectivity of 0.25.
 */
template <typename T>
T get_search_value(const std::string column_name, const size_t row_count_lineitem) {
  std::ofstream compression_selection_selectivities;
  compression_selection_selectivities.open("compression_selection_filters.csv", std::ios_base::app);

  auto sql_pipeline = SQLPipelineBuilder{"SELECT " + column_name + ", count(*) FROM lineitem group by " + column_name + " order by " + column_name + " desc"}.create_pipeline();
  const auto [pipeline_status, table] = sql_pipeline.get_result_table();
  // Print::print(table);
  auto search_value = table->get_value<T>(ColumnID{0}, 0u).value();
  auto accumulated_qualified_rows = table->get_value<int64_t>(ColumnID{1}, 0u).value();
  Assert(table->row_count() > 1, "Unexpected column with only a single value");
  const auto target_filter_rows = static_cast<int64_t>(SELECTIVITY * static_cast<float>(row_count_lineitem));
  for (auto row_id = size_t{1}; row_id < table->row_count(); ++row_id) {
    const auto rows_per_value = table->get_value<int64_t>(ColumnID{1}, row_id).value();

    // 1st case: new added value still does not reach the limit, accept it
    // 2nd case: new added value is nearer to 0.05f and we thus accept it
    if (accumulated_qualified_rows + rows_per_value <= target_filter_rows ||
        abs(accumulated_qualified_rows - target_filter_rows) > abs(accumulated_qualified_rows + rows_per_value - target_filter_rows)) {
      search_value = table->get_value<T>(ColumnID{0}, row_id).value();
      accumulated_qualified_rows += rows_per_value;
      continue;
    }
    break;
  }
  std::cout << "Searching " + column_name + " for »" << search_value << "«." << std::endl;
  compression_selection_selectivities << column_name << "," << row_count_lineitem << "," << table->row_count() << "," << static_cast<double>(accumulated_qualified_rows) / static_cast<double>(row_count_lineitem) << "\n";

  compression_selection_selectivities.close();

  return search_value;
}

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
    compression_selection_selectivities.close();

    const auto row_count_lineitem = lineitem_table->row_count();

    const auto search_value_l_linenumber = get_search_value<int>("l_linenumber", row_count_lineitem);
    const auto search_value_l_partkey = get_search_value<int>("l_partkey", row_count_lineitem);
    const auto search_value_l_shipmode = get_search_value<pmr_string>("l_shipmode", row_count_lineitem);
    const auto search_value_l_comment = get_search_value<pmr_string>("l_comment", row_count_lineitem);

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
    _l_linenumber_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, _l_linenumber_operand,
                                                                          value_(search_value_l_linenumber));

    _l_partkey_operand = pqp_column_(ColumnID{1}, lineitem_table->column_data_type(ColumnID{1}),
                                                lineitem_table->column_is_nullable(ColumnID{1}), "");
    _l_partkey_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, _l_partkey_operand,
                                                                          value_(search_value_l_partkey));

    _last_column_id_lineitem = static_cast<uint16_t>(lineitem_table->column_count()) - 1;
    _l_comment_operand = pqp_column_(ColumnID{_last_column_id_lineitem}, lineitem_table->column_data_type(ColumnID{_last_column_id_lineitem}),
                                                lineitem_table->column_is_nullable(ColumnID{_last_column_id_lineitem}), "");
    _l_comment_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, _l_comment_operand,
                                                                       value_(search_value_l_comment));

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
  }

  return result.str();
}

void csv_measurement_row(const std::string benchmark_name, const SegmentEncodingSpec& seg_spec,
  const size_t repetitions, const size_t value, const size_t nth_of_a_second, std::ofstream& output_file) {
  std::stringstream result;

  result << benchmark_name << "," << segment_encoding_to_csv_string(seg_spec) << "," << repetitions << "," << value;
  result << "," << nth_of_a_second << "\n";

  output_file << result.str();
}

template <typename D>
void csv_measurement_row(const std::string benchmark_name, const SegmentEncodingSpec& seg_spec,
  const size_t repetitions, const D begin, const D end, const size_t nth_of_a_second, std::ofstream& output_file) {

  const auto duration = std::chrono::duration_cast<duration_unit>(end - begin);
  csv_measurement_row(benchmark_name, seg_spec, repetitions, duration.count(), nth_of_a_second, output_file);
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
                                           SegmentEncodingSpec{EncodingType::FixedStringDictionary, VectorCompressionType::FixedSizeByteAligned},
                                           SegmentEncodingSpec{EncodingType::FixedStringDictionary, VectorCompressionType::SimdBp128},
                                           SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::FixedSizeByteAligned},
                                           SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::SimdBp128}};

  // constexpr size_t encoding_measurements = 20;
  // constexpr size_t FILTER_MEASUREMENTS = 1'000;

  constexpr size_t encoding_measurements = 5;
  constexpr size_t FILTER_MEASUREMENTS = 10;

  constexpr size_t nth_of_a_second = 1'000'000;

  std::ofstream measurements;
  measurements.open("measurements.csv");
  measurements << "Benchmark,ColumnName,Encoding,VectorCompression,Repetitions,Value,NthOfASecond\n";

  for (const auto& seg_spec : all_test_segment_encodings) {
    // Both integer columns to encode
    if (seg_spec.encoding_type != EncodingType::FixedStringDictionary) {
      chunk_spec[1] = seg_spec;
      chunk_spec[3] = seg_spec;
    }
    // String columns to encode
    if (seg_spec.encoding_type != EncodingType::FrameOfReference) {
      chunk_spec[10] = seg_spec;
      chunk_spec.back() = seg_spec;
    }
    ChunkEncoder::encode_all_chunks(table, chunk_spec);


    for (auto i = size_t{0}; i < encoding_measurements; ++i) {

      // Writing NULL measurements for Unencoded (no encoding necessary)
      if (seg_spec.encoding_type == EncodingType::Unencoded) {
        {
          const auto start = std::chrono::high_resolution_clock::now();
          csv_measurement_row("Unencoding,l_partkey", seg_spec, encoding_measurements, start, start, nth_of_a_second, measurements);
          csv_measurement_row("Encoding,l_partkey", seg_spec, encoding_measurements, start, start, nth_of_a_second, measurements);
          csv_measurement_row("Unencoding,l_comment", seg_spec, encoding_measurements, start, start, nth_of_a_second, measurements);
          csv_measurement_row("Encoding,l_comment", seg_spec, encoding_measurements, start, start, nth_of_a_second, measurements);
          break;
        }
      }

      const auto measure_encoding_time = [&](const std::string column_name) {
        if (encoding_supports_data_type(seg_spec.encoding_type, LINEITEM_COLUMNS_TO_PROCESS.at(column_name).second)) {
          const auto column_id = LINEITEM_COLUMNS_TO_PROCESS.at(column_name).first;

          chunk_spec[column_id] = SegmentEncodingSpec{EncodingType::Unencoded};
          {
            const auto start = std::chrono::high_resolution_clock::now();
            ChunkEncoder::encode_all_chunks(table, chunk_spec);
            const auto end = std::chrono::high_resolution_clock::now();
            csv_measurement_row("Unencoding," + column_name, seg_spec, encoding_measurements, start, end, nth_of_a_second, measurements);
          }
          chunk_spec[column_id] = seg_spec;
          {
            const auto start = std::chrono::high_resolution_clock::now();
            ChunkEncoder::encode_all_chunks(table, chunk_spec);
            const auto end = std::chrono::high_resolution_clock::now();
            csv_measurement_row("Encoding," + column_name, seg_spec, encoding_measurements, start, end, nth_of_a_second, measurements);
          }
        }
      };

      measure_encoding_time("l_partkey");
      measure_encoding_time("l_linenumber");
      measure_encoding_time("l_shipmode");
      measure_encoding_time("l_comment");
    }

    /**
     *
     * DIRECT SCANS
     *
     */
    const auto search_value_l_linenumber = get_search_value<int>("l_linenumber", table->row_count());
    const auto search_value_l_partkey = get_search_value<int>("l_partkey", table->row_count());
    const auto search_value_l_shipmode = get_search_value<pmr_string>("l_shipmode", table->row_count());
    const auto search_value_l_comment = get_search_value<pmr_string>("l_comment", table->row_count());

    // template <typename D>
    const auto measure_scan = [&](const std::string benchmark, const std::string column_name,
                                         const auto& ref_table, const auto search_value) {
      if (encoding_supports_data_type(seg_spec.encoding_type, LINEITEM_COLUMNS_TO_PROCESS.at(column_name).second)) {
        const auto column_id = LINEITEM_COLUMNS_TO_PROCESS.at(column_name).first;
        const auto operand = pqp_column_(column_id, table->column_data_type(column_id),
                                         table->column_is_nullable(column_id), column_name);
        const auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals,
                                                                           operand, value_(search_value));
        for (auto i = size_t{0}; i < FILTER_MEASUREMENTS; ++i) {
          const auto table_scan = std::make_shared<TableScan>(ref_table, predicate);

          {
            auto start = std::chrono::high_resolution_clock::now();
            table_scan->execute();
            auto end = std::chrono::high_resolution_clock::now();

            csv_measurement_row(benchmark + "," + column_name, seg_spec, FILTER_MEASUREMENTS, start, end, nth_of_a_second, measurements);

            if (benchmark == "DirectScan") {
              const auto check_lineitem_table_size = static_cast<float>(ref_table->get_output()->row_count());
              const auto check_result_table_size = static_cast<float>(table_scan->get_output()->row_count());
              Assert(check_result_table_size > (0.075f * check_lineitem_table_size)
                     && check_result_table_size < (0.20f * check_lineitem_table_size),
                     "Unexpected selectivity (input table size: " + 
                     std::to_string(check_lineitem_table_size) + " and filter " +
                     " result size: " + std::to_string(check_result_table_size) +  ")");
            }
          }
        }
      }
    };
  
    const auto table_wrapper = _table_wrapper_map.at("lineitem");
    measure_scan("DirectScan", "l_partkey", table_wrapper, search_value_l_partkey);
    measure_scan("DirectScan", "l_linenumber", table_wrapper, search_value_l_linenumber);
    measure_scan("DirectScan", "l_shipmode", table_wrapper, search_value_l_shipmode);
    measure_scan("DirectScan", "l_comment", table_wrapper, search_value_l_comment);

    /**
     *
     * REFERENCE SCANS
     *
     */
    const auto position_gap_size = 1.0f / SELECTIVITY;
    std::mt19937 generator(17);
    std::normal_distribution<float> distribution(position_gap_size, position_gap_size * 0.2f);


    // Sorted PosList
    std::shared_ptr<Table> reference_table = std::make_shared<Table>(table->column_definitions(), TableType::References);

    // Creata a reference table of single-chunk-ref chunks
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      auto pos_list = std::make_shared<RowIDPosList>();
      auto row_id = static_cast<uint32_t>(distribution(generator));
      while (row_id < table->get_chunk(chunk_id)->size()) {
        const auto chunk_offset = row_id % table->target_chunk_size();
        pos_list->emplace_back(RowID{chunk_id, ChunkOffset{chunk_offset}});
        row_id += static_cast<uint32_t>(distribution(generator));
      }

      pos_list->guarantee_single_chunk();
      Segments segments;
      for (auto column_id = ColumnID{0}; column_id < reference_table->column_count(); ++column_id) {
        segments.emplace_back(std::make_shared<ReferenceSegment>(table, column_id, pos_list));
      }

      reference_table->append_chunk(segments);  
    }
    auto reference_table_wrapper = std::make_shared<TableWrapper>(std::move(reference_table));
    reference_table_wrapper->execute();

    // Unsorted PosList
    std::shared_ptr<Table> unsorted_reference_table = std::make_shared<Table>(table->column_definitions(), TableType::References);

    auto pos_list = std::make_shared<RowIDPosList>();
    auto row_id = static_cast<uint32_t>(distribution(generator));
    while (row_id < table->row_count()) {
      const auto chunk_id = static_cast<uint32_t>(row_id / table->target_chunk_size());
      const auto chunk_offset = row_id % table->target_chunk_size();
      pos_list->emplace_back(RowID{ChunkID{chunk_id}, ChunkOffset{chunk_offset}});
      row_id += static_cast<uint32_t>(distribution(generator));
    }
    std::shuffle(pos_list->begin(), pos_list->end(), generator);
    Segments unsorted_segments;
    for (auto column_id = ColumnID{0}; column_id < unsorted_reference_table->column_count(); ++column_id) {
      unsorted_segments.emplace_back(std::make_shared<ReferenceSegment>(table, column_id, pos_list));
    }
    unsorted_reference_table->append_chunk(unsorted_segments);  

    auto unsorted_reference_table_wrapper = std::make_shared<TableWrapper>(std::move(unsorted_reference_table));
    unsorted_reference_table_wrapper->execute();

    // auto measure_post_filter = [&](const std::string benchmark, const auto& ref_table, const std::string column_name, const auto predicate) {
    //   if (encoding_supports_data_type(seg_spec.encoding_type, LINEITEM_COLUMNS_TO_PROCESS.at(column_name).second)) {
    //     for (auto i = size_t{0}; i < FILTER_MEASUREMENTS; ++i) {
    //       const auto postfilter_scan = std::make_shared<TableScan>(ref_table, predicate);
          
    //       auto start = std::chrono::high_resolution_clock::now();
    //       postfilter_scan->execute();
    //       auto end = std::chrono::high_resolution_clock::now();
    //       csv_measurement_row(benchmark + "," + column_name, seg_spec, FILTER_MEASUREMENTS, start, end, nth_of_a_second, measurements);
    //     }
    //   }
    // };

    // measure_post_filter("PostFilter", reference_table_wrapper, "l_partkey", _l_partkey_predicate);
    // measure_post_filter("PostFilter", reference_table_wrapper, "l_linenumber", _l_linenumber_predicate);
    // measure_post_filter("PostFilter", reference_table_wrapper, "l_shipmode", _l_shipmode_predicate);
    // measure_post_filter("PostFilter", reference_table_wrapper, "l_comment", _l_comment_predicate);

    // measure_post_filter("UnsortedPostFilter", unsorted_reference_table_wrapper, "l_partkey", _l_partkey_predicate);
    // measure_post_filter("UnsortedPostFilter", unsorted_reference_table_wrapper, "l_linenumber", _l_linenumber_predicate);
    // measure_post_filter("UnsortedPostFilter", unsorted_reference_table_wrapper, "l_shipmode", _l_shipmode_predicate);
    // measure_post_filter("UnsortedPostFilter", unsorted_reference_table_wrapper, "l_comment", _l_comment_predicate);

    measure_scan("PostFilter", "l_partkey", reference_table_wrapper, search_value_l_partkey);
    measure_scan("PostFilter", "l_linenumber", reference_table_wrapper, search_value_l_linenumber);
    measure_scan("PostFilter", "l_shipmode", reference_table_wrapper, search_value_l_shipmode);
    measure_scan("PostFilter", "l_comment", reference_table_wrapper, search_value_l_comment);

    measure_scan("UnsortedPostFilter", "l_partkey", unsorted_reference_table_wrapper, search_value_l_partkey);
    measure_scan("UnsortedPostFilter", "l_linenumber", unsorted_reference_table_wrapper, search_value_l_linenumber);
    measure_scan("UnsortedPostFilter", "l_shipmode", unsorted_reference_table_wrapper, search_value_l_shipmode);
    measure_scan("UnsortedPostFilter", "l_comment", unsorted_reference_table_wrapper, search_value_l_comment);


    auto get_column_size = [&](const std::string column_name) {
      const auto column_id = LINEITEM_COLUMNS_TO_PROCESS.at(column_name).first;
      auto column_size = size_t{0};
      for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        column_size += chunk->get_segment(column_id)->memory_usage(MemoryUsageCalculationMode::Full);
      }

      csv_measurement_row("Size," + column_name, seg_spec, FILTER_MEASUREMENTS, column_size, nth_of_a_second, measurements);
    };

    get_column_size("l_partkey");
    get_column_size("l_linenumber");
    get_column_size("l_shipmode");
    get_column_size("l_comment");

    measurements.flush();
  }
  measurements.close();

  _compression_benchmark_done = true;
}

}  // namespace opossum
