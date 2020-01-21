#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "utils/assert.hpp"

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
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
#include "optimizer/strategy/strategy_base_test.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ChunkPruningRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    auto& storage_manager = Hyrise::get().storage_manager;
    auto compressed_table = load_table("resources/test_data/tbl/int_float2.tbl", 2u);
    ChunkEncoder::encode_all_chunks(compressed_table, SegmentEncodingSpec{EncodingType::Dictionary});
    storage_manager.add_table("compressed", compressed_table);

    auto long_compressed_table = load_table("resources/test_data/tbl/25_ints_sorted.tbl", 25u);
    ChunkEncoder::encode_all_chunks(long_compressed_table, SegmentEncodingSpec{EncodingType::Dictionary});
    storage_manager.add_table("long_compressed", long_compressed_table);

    auto run_length_compressed_table = load_table("resources/test_data/tbl/10_ints.tbl", 5u);
    ChunkEncoder::encode_all_chunks(run_length_compressed_table, SegmentEncodingSpec{EncodingType::RunLength});
    storage_manager.add_table("run_length_compressed", run_length_compressed_table);

    auto string_compressed_table = load_table("resources/test_data/tbl/string.tbl", 3u);
    ChunkEncoder::encode_all_chunks(string_compressed_table, SegmentEncodingSpec{EncodingType::Dictionary});
    storage_manager.add_table("string_compressed", string_compressed_table);

    auto fixed_string_compressed_table = load_table("resources/test_data/tbl/string.tbl", 3u);
    ChunkEncoder::encode_all_chunks(fixed_string_compressed_table,
                                    SegmentEncodingSpec{EncodingType::FixedStringDictionary});
    storage_manager.add_table("fixed_string_compressed", fixed_string_compressed_table);

    for (const auto& [name, table] : storage_manager.tables()) {
      generate_chunk_pruning_statistics(table);
    }

    _rule = std::make_shared<ChunkPruningRule>();

    storage_manager.add_table("uncompressed", load_table("resources/test_data/tbl/int_float2.tbl", 10u));
  }

  std::shared_ptr<ChunkPruningRule> _rule;
};

TEST_F(ChunkPruningRuleTest, SimplePruningTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("compressed");

  auto predicate_node =
      std::make_shared<PredicateNode>(greater_than_(LQPColumnReference(stored_table_node, ColumnID{0}), 200));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{1}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);

  EXPECT_TRUE(stored_table_node->table_statistics);

  // clang-format off
  const auto expected_histogram = GenericHistogram<int32_t>{
    std::vector<int32_t>            {12345},
    std::vector<int32_t>            {12345},
    std::vector<HistogramCountType> {2},
    std::vector<HistogramCountType> {1}};
  // clang-format on

  const auto& column_statistics =
      dynamic_cast<AttributeStatistics<int32_t>&>(*stored_table_node->table_statistics->column_statistics[0]);
  const auto& actual_histogram = dynamic_cast<GenericHistogram<int32_t>&>(*column_statistics.histogram);
  EXPECT_EQ(actual_histogram, expected_histogram);
}

TEST_F(ChunkPruningRuleTest, SimpleChunkPruningTestWithColumnPruning) {
  auto stored_table_node = std::make_shared<StoredTableNode>("compressed");
  stored_table_node->set_pruned_column_ids({ColumnID{0}});

  auto predicate_node =
      std::make_shared<PredicateNode>(less_than_(LQPColumnReference(stored_table_node, ColumnID{1}), 400.0f));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{0}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, BetweenPruningTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("compressed");

  auto predicate_node = std::make_shared<PredicateNode>(
      between_inclusive_(LQPColumnReference(stored_table_node, ColumnID{1}), 350.0f, 351.0f));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{0}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, NoStatisticsAvailable) {
  auto table = Hyrise::get().storage_manager.get_table("uncompressed");
  auto chunk = table->get_chunk(ChunkID(0));
  EXPECT_FALSE(chunk->pruning_statistics());

  auto stored_table_node = std::make_shared<StoredTableNode>("uncompressed");

  auto predicate_node =
      std::make_shared<PredicateNode>(greater_than_(LQPColumnReference(stored_table_node, ColumnID{0}), 200));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, TwoOperatorPruningTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("compressed");

  auto predicate_node_0 =
      std::make_shared<PredicateNode>(greater_than_(LQPColumnReference(stored_table_node, ColumnID{0}), 200));
  predicate_node_0->set_left_input(stored_table_node);

  auto predicate_node_1 =
      std::make_shared<PredicateNode>(less_than_equals_(LQPColumnReference(stored_table_node, ColumnID{1}), 400.0f));
  predicate_node_1->set_left_input(predicate_node_0);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node_1);

  EXPECT_EQ(pruned, predicate_node_1);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{0}, ChunkID{1}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, IntersectionPruningTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("compressed");

  auto predicate_node_0 =
      std::make_shared<PredicateNode>(less_than_(LQPColumnReference(stored_table_node, ColumnID{0}), 10));
  predicate_node_0->set_left_input(stored_table_node);

  auto predicate_node_1 =
      std::make_shared<PredicateNode>(greater_than_(LQPColumnReference(stored_table_node, ColumnID{0}), 200));
  predicate_node_1->set_left_input(stored_table_node);

  auto union_node = std::make_shared<UnionNode>(UnionMode::Positions);
  union_node->set_left_input(predicate_node_0);
  union_node->set_right_input(predicate_node_1);

  auto pruned = StrategyBaseTest::apply_rule(_rule, union_node);

  EXPECT_EQ(pruned, union_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{1}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, ComparatorEdgeCasePruningTest_GreaterThan) {
  auto stored_table_node = std::make_shared<StoredTableNode>("compressed");

  auto predicate_node =
      std::make_shared<PredicateNode>(greater_than_(LQPColumnReference(stored_table_node, ColumnID{0}), 12345));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{0}, ChunkID{1}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, ComparatorEdgeCasePruningTest_Equals) {
  auto stored_table_node = std::make_shared<StoredTableNode>("compressed");

  auto predicate_node =
      std::make_shared<PredicateNode>(equals_(LQPColumnReference(stored_table_node, ColumnID{1}), 458.7f));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{0}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, RangeFilterTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("compressed");

  auto predicate_node =
      std::make_shared<PredicateNode>(equals_(LQPColumnReference(stored_table_node, ColumnID{0}), 50));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{0}, ChunkID{1}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, LotsOfRangesFilterTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("long_compressed");

  auto predicate_node =
      std::make_shared<PredicateNode>(equals_(LQPColumnReference(stored_table_node, ColumnID{0}), 2500));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{0}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, RunLengthSegmentPruningTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("run_length_compressed");

  auto predicate_node = std::make_shared<PredicateNode>(equals_(LQPColumnReference(stored_table_node, ColumnID{0}), 2));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{0}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, GetTablePruningTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("compressed");

  auto predicate_node =
      std::make_shared<PredicateNode>(greater_than_(LQPColumnReference(stored_table_node, ColumnID{0}), 200));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{1}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);

  LQPTranslator translator;
  auto get_table_operator = std::dynamic_pointer_cast<GetTable>(translator.translate_node(stored_table_node));
  EXPECT_TRUE(get_table_operator);

  get_table_operator->execute();
  auto result_table = get_table_operator->get_output();

  EXPECT_EQ(result_table->chunk_count(), ChunkID{1});
  EXPECT_EQ(result_table->get_value<int32_t>(ColumnID{0}, 0), 12345);
}

TEST_F(ChunkPruningRuleTest, StringPruningTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("string_compressed");

  auto predicate_node =
      std::make_shared<PredicateNode>(equals_(LQPColumnReference(stored_table_node, ColumnID{0}), "zzz"));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{0}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, FixedStringPruningTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("fixed_string_compressed");

  auto predicate_node =
      std::make_shared<PredicateNode>(equals_(LQPColumnReference(stored_table_node, ColumnID{0}), "zzz"));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{0}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, PrunePastNonFilteringNodes) {
  auto stored_table_node = std::make_shared<StoredTableNode>("compressed");

  const auto a = stored_table_node->get_column("a");
  const auto b = stored_table_node->get_column("b");

  // clang-format off
  auto input_lqp =
  PredicateNode::make(greater_than_(a, 200),
    ProjectionNode::make(expression_vector(b, a),
      SortNode::make(expression_vector(b), std::vector<OrderByMode>{OrderByMode::Ascending},
        ValidateNode::make(
          stored_table_node))));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_EQ(actual_lqp, input_lqp);

  std::vector<ChunkID> expected_chunk_ids = {ChunkID{1}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningRuleTest, ValueOutOfRange) {
  // Filters are not required to handle values out of their data type's range and the ColumnPruningRule currently
  // doesn't convert out-of-range values into the type's range
  // TODO(anybody) In the test LQP below, the ChunkPruningRule could convert the -3'000'000'000 to MIN_INT (but ONLY
  //               as long as the predicate_condition is >= and not >).

  auto stored_table_node = std::make_shared<StoredTableNode>("compressed");

  // clang-format off
  auto input_lqp =
  PredicateNode::make(greater_than_equals_(LQPColumnReference(stored_table_node, ColumnID{0}), int64_t{-3'000'000'000}),
    stored_table_node);
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_EQ(actual_lqp, input_lqp);
  std::vector<ChunkID> expected_chunk_ids = {};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();

  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

}  // namespace opossum
