#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "operators/get_table.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "strategy_base_test.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class ChunkPruningRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    auto& storage_manager = Hyrise::get().storage_manager;

    auto compressed_table = load_table("resources/test_data/tbl/int_float2.tbl", ChunkOffset{2});
    ChunkEncoder::encode_all_chunks(compressed_table, SegmentEncodingSpec{EncodingType::Dictionary});
    storage_manager.add_table("compressed", compressed_table);

    auto long_compressed_table = load_table("resources/test_data/tbl/25_ints_sorted.tbl", ChunkOffset{25});
    ChunkEncoder::encode_all_chunks(long_compressed_table, SegmentEncodingSpec{EncodingType::Dictionary});
    storage_manager.add_table("long_compressed", long_compressed_table);

    auto run_length_compressed_table = load_table("resources/test_data/tbl/10_ints.tbl", ChunkOffset{5});
    ChunkEncoder::encode_all_chunks(run_length_compressed_table, SegmentEncodingSpec{EncodingType::RunLength});
    storage_manager.add_table("run_length_compressed", run_length_compressed_table);

    auto string_compressed_table = load_table("resources/test_data/tbl/string.tbl", ChunkOffset{3});
    ChunkEncoder::encode_all_chunks(string_compressed_table, SegmentEncodingSpec{EncodingType::Dictionary});
    storage_manager.add_table("string_compressed", string_compressed_table);

    auto fixed_string_compressed_table = load_table("resources/test_data/tbl/string.tbl", ChunkOffset{3});
    ChunkEncoder::encode_all_chunks(fixed_string_compressed_table,
                                    SegmentEncodingSpec{EncodingType::FixedStringDictionary});
    storage_manager.add_table("fixed_string_compressed", fixed_string_compressed_table);

    auto int_float4 = load_table("resources/test_data/tbl/int_float4.tbl", ChunkOffset{2});
    ChunkEncoder::encode_all_chunks(int_float4, SegmentEncodingSpec{EncodingType::Dictionary});
    storage_manager.add_table("int_float4", int_float4);

    for (const auto& [name, table] : storage_manager.tables()) {
      generate_chunk_pruning_statistics(table);
    }

    _rule = std::make_shared<ChunkPruningRule>();

    storage_manager.add_table("uncompressed", load_table("resources/test_data/tbl/int_float2.tbl", ChunkOffset{10}));
  }

  std::shared_ptr<ChunkPruningRule> _rule;
};

TEST_F(ChunkPruningRuleTest, SimplePruningTest) {
  const auto stored_table_node = StoredTableNode::make("compressed");

  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_(lqp_column_(stored_table_node, ColumnID{0}), 200),
    stored_table_node);
  // clang-format on

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{1}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);

  ASSERT_TRUE(stored_table_node->table_statistics);

  // clang-format off
  const auto expected_histogram = GenericHistogram<int32_t>{
    std::vector<int32_t>            {12345},
    std::vector<int32_t>            {12345},
    std::vector<HistogramCountType> {2},
    std::vector<HistogramCountType> {1}};
  // clang-format on

  const auto& column_statistics =
      dynamic_cast<const AttributeStatistics<int32_t>&>(*stored_table_node->table_statistics->column_statistics[0]);
  const auto& actual_histogram = dynamic_cast<const GenericHistogram<int32_t>&>(*column_statistics.histogram);
  EXPECT_EQ(actual_histogram, expected_histogram);
}

TEST_F(ChunkPruningRuleTest, SimpleChunkPruningTestWithColumnPruning) {
  const auto stored_table_node = StoredTableNode::make("compressed");
  stored_table_node->set_pruned_column_ids({ColumnID{0}});

  // clang-format off
  _lqp =
  PredicateNode::make(less_than_(lqp_column_(stored_table_node, ColumnID{1}), 400.0f),
    stored_table_node);
  // clang-format on

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, MultipleOutputs1) {
  // If a temporary table is used more than once, only prune for the predicates that apply to all paths.
  const auto stored_table_node = StoredTableNode::make("int_float4");

  const auto a = lqp_column_(stored_table_node, ColumnID{0});
  const auto b = lqp_column_(stored_table_node, ColumnID{1});

  // clang-format off
  const auto common =
  PredicateNode::make(greater_than_(b, 700),    // Allows for pruning of chunk 0.
    PredicateNode::make(greater_than_(a, 123),  // Allows for pruning of chunk 2.
      stored_table_node));
  _lqp =
  UnionNode::make(SetOperationMode::All,
    PredicateNode::make(less_than_(b, 850),     // Would allow for pruning of chunk 3.
      common),
    PredicateNode::make(greater_than_(b, 850),  // Would allow for pruning of chunk 1.
      common));
  // clang-format on

  _apply_rule(_rule, _lqp);
  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}, ChunkID{2}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, MultipleOutputs2) {
  // Similar to MultipleOutputs1, but b > 700 is now part of one of the branches and can't be used for pruning anymore.
  const auto stored_table_node = StoredTableNode::make("int_float4");

  const auto a = lqp_column_(stored_table_node, ColumnID{0});
  const auto b = lqp_column_(stored_table_node, ColumnID{1});

  // clang-format off
  const auto common =
  PredicateNode::make(greater_than_(a, 123),      // Predicate allows for pruning of chunk 2.
    stored_table_node);
  _lqp =
  UnionNode::make(SetOperationMode::All,
    PredicateNode::make(greater_than_(b, 700),    // Predicate allows for pruning of chunk 0, 2.
      PredicateNode::make(less_than_(b, 850),     // Predicate allows for pruning of chunk 3.
        common)),
    PredicateNode::make(greater_than_(b, 850),    // Predicate allows for pruning of chunk 0, 1, 2.
      common));
  // clang-format on

  _apply_rule(_rule, _lqp);
  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}, ChunkID{2}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, BetweenPruningTest) {
  const auto stored_table_node = StoredTableNode::make("compressed");

  _lqp = PredicateNode::make(between_inclusive_(lqp_column_(stored_table_node, ColumnID{1}), 350.0f, 351.0f));
  _lqp->set_left_input(stored_table_node);

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, NoStatisticsAvailable) {
  const auto table = Hyrise::get().storage_manager.get_table("uncompressed");
  const auto chunk = table->get_chunk(ChunkID(0));
  EXPECT_TRUE(chunk->pruning_statistics());
  chunk->set_pruning_statistics(std::nullopt);
  EXPECT_FALSE(chunk->pruning_statistics());

  const auto stored_table_node = StoredTableNode::make("uncompressed");

  _lqp = PredicateNode::make(greater_than_(lqp_column_(stored_table_node, ColumnID{0}), 200));
  _lqp->set_left_input(stored_table_node);

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, TwoOperatorPruningTest) {
  const auto stored_table_node = StoredTableNode::make("compressed");

  const auto predicate_node = PredicateNode::make(greater_than_(lqp_column_(stored_table_node, ColumnID{0}), 200));
  predicate_node->set_left_input(stored_table_node);

  _lqp = PredicateNode::make(less_than_equals_(lqp_column_(stored_table_node, ColumnID{1}), 400.0f));
  _lqp->set_left_input(predicate_node);

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}, ChunkID{1}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, IntersectionPruningTest) {
  const auto stored_table_node = StoredTableNode::make("compressed");

  const auto predicate_node_0 = PredicateNode::make(less_than_(lqp_column_(stored_table_node, ColumnID{0}), 10));
  predicate_node_0->set_left_input(stored_table_node);

  const auto predicate_node_1 = PredicateNode::make(greater_than_(lqp_column_(stored_table_node, ColumnID{0}), 200));
  predicate_node_1->set_left_input(stored_table_node);

  _lqp = UnionNode::make(SetOperationMode::Positions);
  _lqp->set_left_input(predicate_node_0);
  _lqp->set_right_input(predicate_node_1);

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{1}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, ComparatorEdgeCasePruningTest_GreaterThan) {
  const auto stored_table_node = StoredTableNode::make("compressed");

  _lqp = PredicateNode::make(greater_than_(lqp_column_(stored_table_node, ColumnID{0}), 12345));
  _lqp->set_left_input(stored_table_node);

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}, ChunkID{1}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, ComparatorEdgeCasePruningTest_Equals) {
  const auto stored_table_node = StoredTableNode::make("compressed");

  _lqp = PredicateNode::make(equals_(lqp_column_(stored_table_node, ColumnID{1}), 458.7f));
  _lqp->set_left_input(stored_table_node);

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, RangeFilterTest) {
  const auto stored_table_node = StoredTableNode::make("compressed");

  _lqp = PredicateNode::make(equals_(lqp_column_(stored_table_node, ColumnID{0}), 50));
  _lqp->set_left_input(stored_table_node);

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}, ChunkID{1}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, LotsOfRangesFilterTest) {
  const auto stored_table_node = StoredTableNode::make("long_compressed");

  _lqp = PredicateNode::make(equals_(lqp_column_(stored_table_node, ColumnID{0}), 2500));
  _lqp->set_left_input(stored_table_node);

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, RunLengthSegmentPruningTest) {
  const auto stored_table_node = StoredTableNode::make("run_length_compressed");

  _lqp = PredicateNode::make(equals_(lqp_column_(stored_table_node, ColumnID{0}), 2));
  _lqp->set_left_input(stored_table_node);

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, GetTablePruningTest) {
  const auto stored_table_node = StoredTableNode::make("compressed");

  _lqp = PredicateNode::make(greater_than_(lqp_column_(stored_table_node, ColumnID{0}), 200));
  _lqp->set_left_input(stored_table_node);

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{1}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);

  const auto lqp_translator = LQPTranslator{};
  const auto get_table_operator = std::dynamic_pointer_cast<GetTable>(lqp_translator.translate_node(stored_table_node));
  EXPECT_TRUE(get_table_operator);

  get_table_operator->execute();
  const auto result_table = get_table_operator->get_output();

  EXPECT_EQ(result_table->chunk_count(), ChunkID{1});
  EXPECT_EQ(result_table->get_value<int32_t>(ColumnID{0}, 0), 12345);
}

TEST_F(ChunkPruningRuleTest, StringPruningTest) {
  const auto stored_table_node = StoredTableNode::make("string_compressed");

  _lqp = PredicateNode::make(equals_(lqp_column_(stored_table_node, ColumnID{0}), "zzz"));
  _lqp->set_left_input(stored_table_node);

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, FixedStringPruningTest) {
  const auto stored_table_node = StoredTableNode::make("fixed_string_compressed");

  _lqp = PredicateNode::make(equals_(lqp_column_(stored_table_node, ColumnID{0}), "zzz"));
  _lqp->set_left_input(stored_table_node);

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, PrunePastNonFilteringNodes) {
  const auto stored_table_node = StoredTableNode::make("compressed");

  const auto a = stored_table_node->get_column("a");
  const auto b = stored_table_node->get_column("b");

  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_(a, 200),
    ProjectionNode::make(expression_vector(b, a),
      SortNode::make(expression_vector(b), std::vector<SortMode>{SortMode::Ascending},
        ValidateNode::make(
          stored_table_node))));
  // clang-format on

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{1}};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, PrunePastJoinNodes) {
  const auto stored_table_node_1 = StoredTableNode::make("compressed");
  const auto stored_table_node_2 = StoredTableNode::make("int_float4");

  const auto table_1_a = stored_table_node_1->get_column("a");
  const auto table_2_a = stored_table_node_2->get_column("a");
  const auto table_2_b = stored_table_node_2->get_column("b");

  // clang-format off
  _lqp =
  PredicateNode::make(less_than_(table_2_a, 10000),                              // Prune chunk 0 and 1 of table 2.
    JoinNode::make(JoinMode::Cross,
      PredicateNode::make(less_than_(table_1_a, 200), stored_table_node_1),      // Prune chunk 0 of table 1.
      PredicateNode::make(less_than_(table_2_a, 13000), stored_table_node_2)));  // Prune chunk 3 of table 2.
  // clang-format on

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids_table_1 = std::vector<ChunkID>{ChunkID{0}};
  EXPECT_EQ(stored_table_node_1->pruned_chunk_ids(), expected_chunk_ids_table_1);

  const auto expected_chunk_ids_table_2 = std::vector<ChunkID>{ChunkID{0}, ChunkID{1}, ChunkID{3}};
  EXPECT_EQ(stored_table_node_2->pruned_chunk_ids(), expected_chunk_ids_table_2);
}

TEST_F(ChunkPruningRuleTest, ValueOutOfRange) {
  // Filters are not required to handle values out of their data type's range and the ColumnPruningRule currently
  // doesn't convert out-of-range values into the type's range.
  // TODO(anybody) In the test LQP below, the ChunkPruningRule could convert the -3'000'000'000 to MIN_INT (but ONLY
  //               as long as the predicate_condition is >= and not >).

  const auto stored_table_node = StoredTableNode::make("compressed");

  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_equals_(lqp_column_(stored_table_node, ColumnID{0}), int64_t{-3'000'000'000}),
    stored_table_node);
  // clang-format on

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{};
  EXPECT_EQ(stored_table_node->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, PredicateWithUncorrelatedSubquery) {
  // Predicates that have uncorrelated subqueries as arguments cannot be used for pruning as we do not know the
  // predicate values yet. However, such predicates should not prevent us from pruning by other predicates.
  const auto stored_table_node_1 = StoredTableNode::make("compressed");
  const auto stored_table_node_2 = StoredTableNode::make("compressed");

  const auto subquery_1 = ProjectionNode::make(expression_vector(value_(1.1)), DummyTableNode::make());
  const auto subquery_2 = subquery_1->deep_copy();
  const auto subquery_3 = ProjectionNode::make(expression_vector(value_(3.3)), DummyTableNode::make());

  // clang-format off
  // SELECT * FROM compressed WHERE a > 200 AND b = (SELECT 1.1);
  _lqp =
  PredicateNode::make(greater_than_(stored_table_node_1->get_column("a"), value_(200)),  // Prunes chunk 1.
    PredicateNode::make(equals_(stored_table_node_1->get_column("b"), lqp_subquery_(subquery_1)),
      stored_table_node_1));
  _apply_rule(_rule, _lqp);

  // SELECT * FROM compressed WHERE a > 200 AND b BETWEEN (SELECT 1.1) AND (SELECT 3.3);
  // We also switch the order of the predicates to ensure that it does not matter.
  _lqp =
  PredicateNode::make(between_inclusive_(stored_table_node_2->get_column("b"), lqp_subquery_(subquery_2), lqp_subquery_(subquery_3)),  // NOLINT(whitespace/line_length)
    PredicateNode::make(greater_than_(stored_table_node_2->get_column("a"), value_(200)),  // Prunes chunk 1.
      stored_table_node_2));
  // clang-format on

  _apply_rule(_rule, _lqp);

  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{1}};
  EXPECT_EQ(stored_table_node_1->pruned_chunk_ids(), expected_chunk_ids);
  EXPECT_EQ(stored_table_node_2->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, PredicateWithCorrelatedSubquery) {
  // Similar to PredicateWithUncorrelatedSubquery, we cannot use predicates with correlated subqueries as arguments for
  // pruning because their values are not known before execution. However, pruning by other predicates is still
  // possible.
  const auto stored_table_node_1 = StoredTableNode::make("compressed");
  const auto stored_table_node_2 = StoredTableNode::make("int_float4");
  const auto compressed_a = stored_table_node_1->get_column("a");
  const auto compressed_b = stored_table_node_1->get_column("b");

  const auto parameter = correlated_parameter_(ParameterID{0}, compressed_b);

  // clang-format off
  const auto subquery =
  PredicateNode::make(greater_than_(parameter, stored_table_node_2->get_column("b")),
    stored_table_node_2);

  // SELECT * FROM compressed WHERE a < 200 AND a IN (SELECT a FROM int_float4 WHERE compressed.b > b)
  _lqp =
  PredicateNode::make(less_than_(compressed_a, value_(200)),  // Prunes chunk 0.
    PredicateNode::make(in_(compressed_a, lqp_subquery_(subquery, std::make_pair(ParameterID{0}, compressed_b))),
      stored_table_node_1));
  // clang-format on

  _apply_rule(_rule, _lqp);
  const auto expected_chunk_ids = std::vector<ChunkID>{ChunkID{0}};
  EXPECT_EQ(stored_table_node_1->pruned_chunk_ids(), expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, SetPrunableSubqueryScans) {
  const auto stored_table_node_1 = StoredTableNode::make("compressed");
  const auto stored_table_node_1_col_a = stored_table_node_1->get_column("a");
  const auto stored_table_node_2 = StoredTableNode::make("long_compressed");
  const auto stored_table_node_2_col_a = stored_table_node_2->get_column("a");

  // clang-format off
  const auto subquery =
  ProjectionNode::make(expression_vector(max_(stored_table_node_2_col_a)),
    AggregateNode::make(expression_vector(), expression_vector(max_(stored_table_node_2_col_a)),
    stored_table_node_2));

  _lqp =
  PredicateNode::make(greater_than_(stored_table_node_1_col_a, lqp_subquery_(subquery)),
    stored_table_node_1);
  // clang-format on

  _apply_rule(_rule, _lqp);
  EXPECT_TRUE(stored_table_node_1->pruned_chunk_ids().empty());
  ASSERT_EQ(stored_table_node_1->prunable_subquery_predicates().size(), 1);
  EXPECT_EQ(stored_table_node_1->prunable_subquery_predicates().front(), _lqp);
}

TEST_F(ChunkPruningRuleTest, DoNotSetPrunableSubqueryScansWhenNotInAllChains) {
  const auto stored_table_node_1 = StoredTableNode::make("compressed");
  const auto stored_table_node_1_col_a = stored_table_node_1->get_column("a");
  const auto stored_table_node_2 = StoredTableNode::make("long_compressed");
  const auto stored_table_node_2_col_a = stored_table_node_2->get_column("a");

  // clang-format off
  const auto subquery_min =
  ProjectionNode::make(expression_vector(max_(stored_table_node_2_col_a)),
    AggregateNode::make(expression_vector(), expression_vector(max_(stored_table_node_2_col_a)),
    stored_table_node_2));

  const auto subquery_max =
  ProjectionNode::make(expression_vector(max_(stored_table_node_2_col_a)),
    AggregateNode::make(expression_vector(), expression_vector(max_(stored_table_node_2_col_a)),
    stored_table_node_2));

  _lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(equals_(stored_table_node_1_col_a, lqp_subquery_(subquery_min)),
      stored_table_node_1),
    PredicateNode::make(between_inclusive_(stored_table_node_1_col_a, lqp_subquery_(subquery_min), lqp_subquery_(subquery_max)),  // NOLINT(whitespace/line_length)
      stored_table_node_1));
  // clang-format on

  _apply_rule(_rule, _lqp);
  EXPECT_TRUE(stored_table_node_1->pruned_chunk_ids().empty());
  ASSERT_TRUE(stored_table_node_1->prunable_subquery_predicates().empty());
}

TEST_F(ChunkPruningRuleTest, DoNotSetPrunableSubqueryScansComplicatedPredicate) {
  const auto stored_table_node_1 = StoredTableNode::make("compressed");
  const auto stored_table_node_1_col_a = stored_table_node_1->get_column("a");
  const auto stored_table_node_2 = StoredTableNode::make("long_compressed");
  const auto stored_table_node_2_col_a = stored_table_node_2->get_column("a");

  // clang-format off
  const auto subquery =
  ProjectionNode::make(expression_vector(stored_table_node_2_col_a),
    AggregateNode::make(expression_vector(stored_table_node_2_col_a), expression_vector(),
    stored_table_node_2));

  _lqp =
  PredicateNode::make(in_(stored_table_node_1_col_a, lqp_subquery_(subquery)),
    stored_table_node_1);
  // clang-format on

  _apply_rule(_rule, _lqp);
  EXPECT_TRUE(stored_table_node_1->pruned_chunk_ids().empty());
  ASSERT_TRUE(stored_table_node_1->prunable_subquery_predicates().empty());
}

}  // namespace hyrise
