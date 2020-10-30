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
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/segment_iterate.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class TableWrapper;

// Defining the base fixture class
class TPCHDataMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
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
  void TearDown(::benchmark::State&) override {}

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


template <typename ColumnDataType,  bool useReference>
void benchmark_segment(const std::shared_ptr<AbstractSegment>& segment, size_t& sum) {
  segment_with_iterators<ColumnDataType>(*segment, [&](auto it, const auto end) {
    using SegmentPositionType = typename decltype(it)::value_type;
    using SegmentPositionType2 = typename std::conditional_t<useReference, const SegmentPositionType&, const SegmentPositionType>;

    while (it != end) {
      const SegmentPositionType2 position = *it;
      if (!position.is_null()) {
        if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
          sum += static_cast<int64_t>(position.value()[0]);
        } else {
          sum += static_cast<int64_t>(position.value()) % 5;
        }
      }
    ++it;
    }
  });
}

void BM_SegmentPositionReference(benchmark::State& state, std::string column_name, const SegmentEncodingSpec segment_encoding_spec, const bool mark_as_non_nullable = false) {
  constexpr auto SCALE_FACTOR = 1.0f;
  auto benchmark_config = BenchmarkConfig::get_default_config();

  if (!Hyrise::get().storage_manager.has_table("lineitem")) {
    std::cout << "Generating TPC-H data set with scale factor " << SCALE_FACTOR << std::endl;
    TPCHTableGenerator(SCALE_FACTOR, std::make_shared<BenchmarkConfig>(benchmark_config)).generate_and_store();
  }

  const auto lineitem_table = Hyrise::get().storage_manager.get_table("lineitem");
  const auto column_id = lineitem_table->column_id_by_name(column_name);

  auto specs = std::vector<SegmentEncodingSpec>(lineitem_table->column_count(), SegmentEncodingSpec{EncodingType::Dictionary});
  specs[column_id] = segment_encoding_spec;
  ChunkEncoder::encode_all_chunks(lineitem_table, specs);

  const auto lineitem_table_wrapper = std::make_shared<TableWrapper>(lineitem_table);
  lineitem_table_wrapper->execute();


  const auto chunk_count = lineitem_table->chunk_count();

  resolve_data_type(lineitem_table->column_data_type(column_id), [&](const auto data_type) {
    using ColumnDataType = typename decltype(data_type)::type;

    for (auto _ : state) {
      auto sum = size_t{0};

      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto& chunk = lineitem_table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(column_id);
	
	if (mark_as_non_nullable) {
	  const auto& value_segment = static_cast<ValueSegment<ColumnDataType>&>(*segment);
          auto new_value_segment = std::make_shared<ValueSegment<ColumnDataType>>(pmr_vector<ColumnDataType>(value_segment.values()));
	  assert(!new_value_segment->is_nullable());
	  benchmark_segment<ColumnDataType, true>(new_value_segment, sum);
	} else {
	  benchmark_segment<ColumnDataType, true>(segment, sum);
	}

      }

      assert(sum > 0);
    }
  });
}

void BM_SegmentPositionPosition(benchmark::State& state, std::string column_name, const SegmentEncodingSpec segment_encoding_spec, const bool mark_as_non_nullable = false) {
  const auto lineitem_table = Hyrise::get().storage_manager.get_table("lineitem");
  const auto lineitem_table_wrapper = std::make_shared<TableWrapper>(lineitem_table);
  lineitem_table_wrapper->execute();

  const auto column_id = lineitem_table->column_id_by_name(column_name);

  const auto chunk_count = lineitem_table->chunk_count();

  resolve_data_type(lineitem_table->column_data_type(column_id), [&](const auto data_type) {
    using ColumnDataType = typename decltype(data_type)::type;

    for (auto _ : state) {
      auto sum = size_t{0};

      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto& chunk = lineitem_table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(column_id);

	if (mark_as_non_nullable) {
	  const auto& value_segment = static_cast<ValueSegment<ColumnDataType>&>(*segment);
          auto new_value_segment = std::make_shared<ValueSegment<ColumnDataType>>(pmr_vector<ColumnDataType>(value_segment.values()));
	  assert(!new_value_segment->is_nullable());
	  benchmark_segment<ColumnDataType, false>(new_value_segment, sum);
	} else {
	  benchmark_segment<ColumnDataType, false>(segment, sum);
	}
      }

      assert(sum > 0);
    }
  });
}



void BM_SegmentPositionNoneManual(benchmark::State& state, std::string column_name) {
  const auto lineitem_table = Hyrise::get().storage_manager.get_table("lineitem");
  const auto lineitem_table_wrapper = std::make_shared<TableWrapper>(lineitem_table);
  lineitem_table_wrapper->execute();

  const auto column_id = lineitem_table->column_id_by_name(column_name);

  const auto chunk_count = lineitem_table->chunk_count();

  resolve_data_type(lineitem_table->column_data_type(column_id), [&](const auto data_type) {
    using ColumnDataType = typename decltype(data_type)::type;

    auto vector = std::vector<std::vector<ColumnDataType>>(lineitem_table->chunk_count());
    auto null_vector = std::vector<std::vector<bool>>(lineitem_table->chunk_count());

    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto& chunk = lineitem_table->get_chunk(chunk_id);
      const auto& segment = chunk->get_segment(column_id);

      //const auto& typed_segment = static_cast<const ValueSegment<ColumnDataType>&>(*segment);
      //std::cout << "using the nonnull? " << typed_segment.is_nullable() << std::endl;

      vector[chunk_id].reserve(chunk->size());
      null_vector[chunk_id].reserve(chunk->size());

      auto offset = size_t{0};
      segment_with_iterators<ColumnDataType>(*segment, [&](auto it, const auto end) {
        while (it != end) {
          const auto position = *it;
          if (!position.is_null()) {
	    vector[chunk_id].push_back(position.value());
	    null_vector[chunk_id].push_back(false);
          } else {
	    null_vector[chunk_id].push_back(true);
	  }
          ++it;
	  ++offset;
        }
      });
    }

    for (auto _ : state) {
      auto sum = size_t{0};

      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
	const auto chunk_vector = vector[chunk_id];

        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_vector.size(); ++chunk_offset) {
          //if (!position.is_null()) {
            if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
              sum += static_cast<int64_t>(chunk_vector[chunk_offset][0]);
            } else {
              sum += static_cast<int64_t>(chunk_vector[chunk_offset]) % 5;
            }
          //}
        }
      }

      assert(sum > 0);
    }
  });
}

void BM_SegmentPositionNone(benchmark::State& state, std::string column_name, const SegmentEncodingSpec segment_encoding_spec) {
  const auto lineitem_table = Hyrise::get().storage_manager.get_table("lineitem");
  const auto lineitem_table_wrapper = std::make_shared<TableWrapper>(lineitem_table);
  lineitem_table_wrapper->execute();

  const auto column_id = lineitem_table->column_id_by_name(column_name);

  const auto chunk_count = lineitem_table->chunk_count();

  resolve_data_type(lineitem_table->column_data_type(column_id), [&](const auto data_type) {
    using ColumnDataType = typename decltype(data_type)::type;

    for (auto _ : state) {
      auto sum = size_t{0};

      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto& chunk = lineitem_table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(column_id);
        const auto& typed_segment = static_cast<const DictionarySegment<ColumnDataType>&>(*segment);

        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk->size(); ++chunk_offset) {
          const auto position = typed_segment.get_typed_value(chunk_offset);
          if (position) {
            if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
              sum += static_cast<int64_t>((*position)[0]);
            } else {
              sum += static_cast<int64_t>(*position) % 5;
            }
          }
        }
      }

      assert(sum > 0);
    }
  });
}

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_shipdate_dict, std::string("l_shipdate"), SegmentEncodingSpec{EncodingType::Dictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_shipdate_dict, std::string("l_shipdate"), SegmentEncodingSpec{EncodingType::Dictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionNone, l_shipdate_dict, std::string("l_shipdate"), SegmentEncodingSpec{EncodingType::Dictionary});

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_returnflag_dict, std::string("l_returnflag"), SegmentEncodingSpec{EncodingType::Dictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_returnflag_dict, std::string("l_returnflag"), SegmentEncodingSpec{EncodingType::Dictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionNone, l_returnflag_dict, std::string("l_returnflag"), SegmentEncodingSpec{EncodingType::Dictionary});

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_extendedprice_dict, std::string("l_extendedprice"), SegmentEncodingSpec{EncodingType::Dictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_extendedprice_dict, std::string("l_extendedprice"), SegmentEncodingSpec{EncodingType::Dictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionNone, l_extendedprice_dict, std::string("l_extendedprice"), SegmentEncodingSpec{EncodingType::Dictionary});

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_discount_dict, std::string("l_discount"), SegmentEncodingSpec{EncodingType::Dictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_discount_dict, std::string("l_discount"), SegmentEncodingSpec{EncodingType::Dictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionNone, l_discount_dict, std::string("l_discount"), SegmentEncodingSpec{EncodingType::Dictionary});

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_linenumber_dict, std::string("l_linenumber"), SegmentEncodingSpec{EncodingType::Dictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_linenumber_dict, std::string("l_linenumber"), SegmentEncodingSpec{EncodingType::Dictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionNone, l_linenumber_dict, std::string("l_linenumber"), SegmentEncodingSpec{EncodingType::Dictionary});

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_orderkey_dict, std::string("l_orderkey"), SegmentEncodingSpec{EncodingType::Dictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_orderkey_dict, std::string("l_orderkey"), SegmentEncodingSpec{EncodingType::Dictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionNone, l_orderkey_dict, std::string("l_orderkey"), SegmentEncodingSpec{EncodingType::Dictionary});


BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_shipdate_unencoded_Default, std::string("l_shipdate"), SegmentEncodingSpec{EncodingType::Unencoded}, false);
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_shipdate_unencoded_Default, std::string("l_shipdate"), SegmentEncodingSpec{EncodingType::Unencoded}, false);
BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_shipdate_unencoded_NonNull, std::string("l_shipdate"), SegmentEncodingSpec{EncodingType::Unencoded}, true);
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_shipdate_unencoded_NonNull, std::string("l_shipdate"), SegmentEncodingSpec{EncodingType::Unencoded}, true);
BENCHMARK_CAPTURE(BM_SegmentPositionNoneManual, l_shipdate_unencoded, std::string("l_shipdate"));

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_returnflag_unencoded_Default, std::string("l_returnflag"), SegmentEncodingSpec{EncodingType::Unencoded}, false);
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_returnflag_unencoded_Default, std::string("l_returnflag"), SegmentEncodingSpec{EncodingType::Unencoded}, false);
BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_returnflag_unencoded_NonNull, std::string("l_returnflag"), SegmentEncodingSpec{EncodingType::Unencoded}, true);
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_returnflag_unencoded_NonNull, std::string("l_returnflag"), SegmentEncodingSpec{EncodingType::Unencoded}, true);
BENCHMARK_CAPTURE(BM_SegmentPositionNoneManual, l_returnflag_unencoded, std::string("l_returnflag"));

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_extendedprice_unencoded_Default, std::string("l_extendedprice"), SegmentEncodingSpec{EncodingType::Unencoded}, false);
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_extendedprice_unencoded_Default, std::string("l_extendedprice"), SegmentEncodingSpec{EncodingType::Unencoded}, false);
BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_extendedprice_unencoded_NonNull, std::string("l_extendedprice"), SegmentEncodingSpec{EncodingType::Unencoded}, true);
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_extendedprice_unencoded_NonNull, std::string("l_extendedprice"), SegmentEncodingSpec{EncodingType::Unencoded}, true);
BENCHMARK_CAPTURE(BM_SegmentPositionNoneManual, l_extendedprice_unencoded, std::string("l_extendedprice"));

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_discount_unencoded_Default, std::string("l_discount"), SegmentEncodingSpec{EncodingType::Unencoded}, false);
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_discount_unencoded_Default, std::string("l_discount"), SegmentEncodingSpec{EncodingType::Unencoded}, false);
BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_discount_unencoded_NonNull, std::string("l_discount"), SegmentEncodingSpec{EncodingType::Unencoded}, true);
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_discount_unencoded_NonNull, std::string("l_discount"), SegmentEncodingSpec{EncodingType::Unencoded}, true);
BENCHMARK_CAPTURE(BM_SegmentPositionNoneManual, l_discount_unencoded, std::string("l_discount"));

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_linenumber_unencoded_Default, std::string("l_linenumber"), SegmentEncodingSpec{EncodingType::Unencoded}, false);
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_linenumber_unencoded_Default, std::string("l_linenumber"), SegmentEncodingSpec{EncodingType::Unencoded}, false);
BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_linenumber_unencoded_NonNull, std::string("l_linenumber"), SegmentEncodingSpec{EncodingType::Unencoded}, true);
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_linenumber_unencoded_NonNull, std::string("l_linenumber"), SegmentEncodingSpec{EncodingType::Unencoded}, true);
BENCHMARK_CAPTURE(BM_SegmentPositionNoneManual, l_linenumber_unencoded, std::string("l_linenumber"));

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_orderkey_unencoded_Default, std::string("l_orderkey"), SegmentEncodingSpec{EncodingType::Unencoded}, false);
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_orderkey_unencoded_Default, std::string("l_orderkey"), SegmentEncodingSpec{EncodingType::Unencoded}, false);
BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_orderkey_unencoded_NonNull, std::string("l_orderkey"), SegmentEncodingSpec{EncodingType::Unencoded}, true);
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_orderkey_unencoded_NonNull, std::string("l_orderkey"), SegmentEncodingSpec{EncodingType::Unencoded}, true);
BENCHMARK_CAPTURE(BM_SegmentPositionNoneManual, l_orderkey_unencoded, std::string("l_orderkey"));


BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_shipdate_lz4, std::string("l_shipdate"), SegmentEncodingSpec{EncodingType::LZ4});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_shipdate_lz4, std::string("l_shipdate"), SegmentEncodingSpec{EncodingType::LZ4});

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_returnflag_lz4, std::string("l_returnflag"), SegmentEncodingSpec{EncodingType::LZ4});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_returnflag_lz4, std::string("l_returnflag"), SegmentEncodingSpec{EncodingType::LZ4});

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_extendedprice_lz4, std::string("l_extendedprice"), SegmentEncodingSpec{EncodingType::LZ4});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_extendedprice_lz4, std::string("l_extendedprice"), SegmentEncodingSpec{EncodingType::LZ4});

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_discount_lz4, std::string("l_discount"), SegmentEncodingSpec{EncodingType::LZ4});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_discount_lz4, std::string("l_discount"), SegmentEncodingSpec{EncodingType::LZ4});

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_linenumber_lz4, std::string("l_linenumber"), SegmentEncodingSpec{EncodingType::LZ4});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_linenumber_lz4, std::string("l_linenumber"), SegmentEncodingSpec{EncodingType::LZ4});

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_orderkey_lz4, std::string("l_orderkey"), SegmentEncodingSpec{EncodingType::LZ4});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_orderkey_lz4, std::string("l_orderkey"), SegmentEncodingSpec{EncodingType::LZ4});



BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_shipdate_fs, std::string("l_shipdate"), SegmentEncodingSpec{EncodingType::FixedStringDictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_shipdate_fs, std::string("l_shipdate"), SegmentEncodingSpec{EncodingType::FixedStringDictionary});

BENCHMARK_CAPTURE(BM_SegmentPositionReference, l_returnflag_fs, std::string("l_returnflag"), SegmentEncodingSpec{EncodingType::FixedStringDictionary});
BENCHMARK_CAPTURE(BM_SegmentPositionPosition, l_returnflag_fs, std::string("l_returnflag"), SegmentEncodingSpec{EncodingType::FixedStringDictionary});

}  // namespace opossum
