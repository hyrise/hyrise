#include <unordered_set>
#include <vector>

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
#include "operators/join_hash.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "storage/dictionary_segment/dictionary_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class TableWrapper;

// Defining the base fixture class.
class TPCHDataMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    auto& sm = Hyrise::get().storage_manager;
    const auto scale_factor = 10.0f;
    const auto benchmark_config = std::make_shared<BenchmarkConfig>();

    if (!sm.has_table("lineitem")) {
      std::cout << "Generating TPC-H data set with scale factor " << scale_factor << " and "
                << benchmark_config->encoding_config.default_encoding_spec << " encoding:\n";
      TPCHTableGenerator(scale_factor, ClusteringConfiguration::None, benchmark_config).generate_and_store();
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
  first_scan->never_clear_output();
  first_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(first_scan, _tpchq6_shipdate_less_predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(TPCHDataMicroBenchmarkFixture, BM_TPCHQ6ThirdScanPredicate)(benchmark::State& state) {
  const auto first_scan = std::make_shared<TableScan>(_table_wrapper_map.at("lineitem"), _tpchq6_discount_predicate);
  first_scan->never_clear_output();
  first_scan->execute();
  const auto first_scan_result = first_scan->get_output();
  const auto second_scan = std::make_shared<TableScan>(first_scan, _tpchq6_shipdate_less_predicate);
  second_scan->never_clear_output();
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
  table_scan->never_clear_output();
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
  table_scan->never_clear_output();
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
  sorted_lineitem->never_clear_output();
  sorted_lineitem->execute();
  const auto mocked_table_scan_output = sorted_lineitem->get_output();
  const auto group_by_column = l_orderkey_id;
  const auto group_by = std::vector<ColumnID>{l_orderkey_id};
  const auto aggregate_expressions = std::vector<std::shared_ptr<WindowFunctionExpression>>{
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
    const auto& [tasks, root_operator_task] = OperatorTask::make_tasks_from_operator(pqp);
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

BENCHMARK_DEFINE_F(TPCHDataMicroBenchmarkFixture, BM_LineitemHistogramCreation)(benchmark::State& state) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  const auto column_id = ColumnID{static_cast<ColumnID::base_type>(state.range(0))};

  const auto& sm = Hyrise::get().storage_manager;
  const auto& lineitem_table = sm.get_table("lineitem");

  const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, lineitem_table->row_count() / 2'000));

  const auto column_data_type = lineitem_table->column_data_type(column_id);

  resolve_data_type(column_data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    for (auto _ : state) {
      EqualDistinctCountHistogram<ColumnDataType>::from_column(*lineitem_table, column_id, histogram_bin_count);
    }
  });
}

BENCHMARK_DEFINE_F(TPCHDataMicroBenchmarkFixture, BM_LineitemManualDictionaryEncoding)(benchmark::State& state) {
  const auto column_id = ColumnID{static_cast<ColumnID::base_type>(state.range(0))};

  const auto& sm = Hyrise::get().storage_manager;
  const auto& lineitem_table = sm.get_table("lineitem");
  const auto chunk_count = lineitem_table->chunk_count();

  // std::cout << "Encoding entire lineitem table as Unencoded ... ";
  ChunkEncoder::encode_all_chunks(lineitem_table, SegmentEncodingSpec{EncodingType::Unencoded});
  // std::cout << "done\n";

  const auto column_data_type = lineitem_table->column_data_type(column_id);

  auto useless_sum = size_t{0};

  resolve_data_type(column_data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    for (auto _ : state) {
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto& chunk = lineitem_table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(column_id);
        const auto chunk_size = chunk->size();

        auto materialized_offets_and_values = std::vector<std::tuple<size_t, bool, ColumnDataType>>{};
        segment_with_iterators<ColumnDataType>(*segment, [&](auto iter, const auto end) {
          auto offset = size_t{0};
          for (; iter != end; ++iter, ++offset) {
            materialized_offets_and_values.emplace_back(offset, iter->is_null(),
                                                        iter->is_null() ? ColumnDataType{} : iter->value());
            if (iter->is_null()) {
              std::cerr << "WWWWWWOW\n";
            }
          }

          // std::cout << "\n\nBEFORE SORTING\n";
          // for (const auto& narf : materialized_offets_and_values) {
          //   std::cout << std::get<0>(narf) << " - " << std::get<1>(narf) << " - " << std::get<2>(narf) << "\n";
          // }

          // Using a stable sort to have a somewhat sequential write pattern when writing the offset later (can be the
          // case with increasing keys or few distinct values, which both regularly occur).
          std::stable_sort(materialized_offets_and_values.begin(), materialized_offets_and_values.end(),
                           [](const auto& lhs, const auto& rhs) {
                             return std::get<1>(rhs) || std::get<2>(lhs) < std::get<2>(rhs);
                           });

          // std::cout << "\n\nAFTER SORTING\n";
          // for (const auto& narf : materialized_offets_and_values) {
          //   std::cout << std::get<0>(narf) << " - " << std::get<1>(narf) << " - " << std::get<2>(narf) << "\n";
          // }

          if (std::get<1>(materialized_offets_and_values[0])) {
            // All values are NULL.
            // TODO(Bossl): Create repeated NULLed AV.
          }

          auto dictionary_values = pmr_vector<ColumnDataType>{};
          auto attribute_vector = pmr_vector<uint32_t>{};
          dictionary_values.reserve(chunk_size / 2);
          attribute_vector.resize(chunk_size);

          auto dictionary_offset = size_t{0};
          auto av_offset = size_t{0};

          auto previous_value = std::get<2>(materialized_offets_and_values[0]);
          dictionary_values.emplace_back(previous_value);

          for (const auto& item : materialized_offets_and_values) {
            const auto& offset = std::get<0>(item);
            const auto& is_null = std::get<1>(item);

            if (is_null) {
              // All following values are NULLs as well.
              attribute_vector[offset] = dictionary_values.size();
              continue;
            }

            const auto& value = std::get<2>(item);
            if (value != previous_value) {
              ++dictionary_offset;
              dictionary_values.emplace_back(value);
              previous_value = value;
            }

            attribute_vector[av_offset] = dictionary_offset;
          }
          dictionary_values.shrink_to_fit();
          // if (chunk_id == ChunkID{0})
          //   std::cout << "dict size: " << dictionary_values.size() << "\n";

          const auto encoded_segment =
              DictionarySegment(std::move(dictionary_values),
                                compress_vector(attribute_vector, VectorCompressionType::FixedWidthInteger,
                                                PolymorphicAllocator<ColumnDataType>{}, {dictionary_values.size()}));
          useless_sum += encoded_segment.size();
        });
      }
    }
  });
}

BENCHMARK_DEFINE_F(TPCHDataMicroBenchmarkFixture, BM_LineitemManualStringOptimizedDictionaryEncoding)
(benchmark::State& state) {
  const auto column_id = ColumnID{static_cast<ColumnID::base_type>(state.range(0))};

  const auto& sm = Hyrise::get().storage_manager;
  const auto& lineitem_table = sm.get_table("lineitem");
  const auto chunk_count = lineitem_table->chunk_count();

  // std::cout << "Encoding entire lineitem table as Unencoded ... ";
  ChunkEncoder::encode_all_chunks(lineitem_table, SegmentEncodingSpec{EncodingType::Unencoded});
  // std::cout << "done\n";

  const auto column_data_type = lineitem_table->column_data_type(column_id);

  auto useless_sum = size_t{0};

  resolve_data_type(column_data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    for (auto _ : state) {
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto& chunk = lineitem_table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(column_id);
        const auto chunk_size = chunk->size();

        const auto segment_encoding_spec = get_segment_encoding_spec(segment);

        using ValueType =
            std::conditional_t<std::is_same_v<ColumnDataType, pmr_string>, std::string_view, ColumnDataType>;

        // We are using the STL garantees of stable item pointers here.
        [[maybe_unused]] auto string_set = std::unordered_set<pmr_string>{};
        if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
          if (segment_encoding_spec != SegmentEncodingSpec{EncodingType::Unencoded}) {
            string_set.reserve(chunk_size / 2);
          }
        }

        auto materialized_offets_and_values = std::vector<std::tuple<size_t, bool, ValueType>>{};
        segment_with_iterators<ColumnDataType>(*segment, [&](auto iter, const auto end) {
          auto offset = size_t{0};
          for (; iter != end; ++iter, ++offset) {
            auto value = ValueType{};
            if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
              if (!iter->is_null()) {
                if (segment_encoding_spec == SegmentEncodingSpec{EncodingType::Unencoded}) {
                  const auto& vs = static_cast<ValueSegment<pmr_string>&>(*segment);
                  value = std::string_view{vs.values()[offset]};
                } else {
                  const auto [set_iter, _] = string_set.emplace(iter->value());
                  value = std::string_view{*set_iter};
                }
              }
            } else {
              if (!iter->is_null()) {
                value = iter->value();
              }
            }
            materialized_offets_and_values.emplace_back(offset, iter->is_null(), std::move(value));
            if (iter->is_null()) {
              std::cerr << "WWWWWWOW\n";
            }
          }
          // Using a stable sort to have a somewhat sequential write pattern when writing the offset later (can be the
          // case with increasing keys or few distinct values, which both regularly occur).
          std::stable_sort(materialized_offets_and_values.begin(), materialized_offets_and_values.end(),
                           [](const auto& lhs, const auto& rhs) {
                             return std::get<1>(rhs) || std::get<2>(lhs) < std::get<2>(rhs);
                           });

          if (std::get<1>(materialized_offets_and_values[0])) {
            // All values are NULL.
            // TODO(Bossl): Create repeated NULLed AV.
          }

          auto dictionary_values = pmr_vector<ColumnDataType>{};
          auto attribute_vector = pmr_vector<uint32_t>{};
          dictionary_values.reserve(chunk_size / 2);
          attribute_vector.resize(chunk_size);

          auto dictionary_offset = size_t{0};
          auto av_offset = size_t{0};

          auto previous_value = std::get<2>(materialized_offets_and_values[0]);
          dictionary_values.emplace_back(previous_value);

          for (const auto& item : materialized_offets_and_values) {
            const auto& offset = std::get<0>(item);
            const auto& is_null = std::get<1>(item);

            if (is_null) {
              // All following values are NULLs as well.
              attribute_vector[offset] = dictionary_values.size();
              continue;
            }

            const auto& value = std::get<2>(item);
            if (value != previous_value) {
              ++dictionary_offset;
              dictionary_values.emplace_back(value);
              previous_value = value;
            }

            attribute_vector[av_offset] = dictionary_offset;
          }
          dictionary_values.shrink_to_fit();
          // std::cout << "dict size: " << dictionary_values.size() << "\n";

          const auto encoded_segment =
              DictionarySegment(std::move(dictionary_values),
                                compress_vector(attribute_vector, VectorCompressionType::FixedWidthInteger,
                                                PolymorphicAllocator<ColumnDataType>{}, {dictionary_values.size()}));
          useless_sum += encoded_segment.size();
        });
      }
    }
  });
}

BENCHMARK_DEFINE_F(TPCHDataMicroBenchmarkFixture, BM_LineitemDictionaryEncoding)(benchmark::State& state) {
  const auto column_id = ColumnID{static_cast<ColumnID::base_type>(state.range(0))};

  const auto& sm = Hyrise::get().storage_manager;
  const auto& lineitem_table = sm.get_table("lineitem");
  const auto chunk_count = lineitem_table->chunk_count();

  // std::cout << "Encoding entire lineitem table as Unencoded ... ";
  ChunkEncoder::encode_all_chunks(lineitem_table, SegmentEncodingSpec{EncodingType::Unencoded});
  // std::cout << "done\n";

  const auto column_data_type = lineitem_table->column_data_type(column_id);

  resolve_data_type(column_data_type, [&](auto type) {
    for (auto _ : state) {
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto& chunk = lineitem_table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(column_id);

        ChunkEncoder::encode_segment(segment, column_data_type, SegmentEncodingSpec{EncodingType::Dictionary});
      }
    }
  });
}

constexpr auto LINEITEM_COLUMN_COUNT = 15;
BENCHMARK_REGISTER_F(TPCHDataMicroBenchmarkFixture, BM_LineitemHistogramCreation)->DenseRange(0, LINEITEM_COLUMN_COUNT);
BENCHMARK_REGISTER_F(TPCHDataMicroBenchmarkFixture, BM_LineitemManualDictionaryEncoding)
    ->DenseRange(0, LINEITEM_COLUMN_COUNT);
BENCHMARK_REGISTER_F(TPCHDataMicroBenchmarkFixture, BM_LineitemManualStringOptimizedDictionaryEncoding)
    ->DenseRange(0, LINEITEM_COLUMN_COUNT);
BENCHMARK_REGISTER_F(TPCHDataMicroBenchmarkFixture, BM_LineitemDictionaryEncoding)
    ->DenseRange(0, LINEITEM_COLUMN_COUNT);

}  // namespace hyrise
