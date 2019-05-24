#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "operators/get_table.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"

#include "utils/assert.hpp"

#include "logical_query_plan/mock_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ChunkPruningTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    auto& storage_manager = StorageManager::get();
    storage_manager.add_table("compressed", load_table("resources/test_data/tbl/int_float2.tbl", 2u));
    storage_manager.add_table("long_compressed", load_table("resources/test_data/tbl/25_ints_sorted.tbl", 25u));
    storage_manager.add_table("run_length_compressed", load_table("resources/test_data/tbl/10_ints.tbl", 5u));
    storage_manager.add_table("string_compressed", load_table("resources/test_data/tbl/string.tbl", 3u));
    storage_manager.add_table("fixed_string_compressed", load_table("resources/test_data/tbl/string.tbl", 3u));

    ChunkEncoder::encode_all_chunks(storage_manager.get_table("compressed"), EncodingType::Dictionary);
    ChunkEncoder::encode_all_chunks(storage_manager.get_table("long_compressed"), EncodingType::Dictionary);
    ChunkEncoder::encode_all_chunks(storage_manager.get_table("run_length_compressed"), EncodingType::RunLength);
    ChunkEncoder::encode_all_chunks(storage_manager.get_table("string_compressed"), EncodingType::Dictionary);
    ChunkEncoder::encode_all_chunks(storage_manager.get_table("fixed_string_compressed"),
                                    EncodingType::FixedStringDictionary);
    _rule = std::make_shared<ChunkPruningRule>();

    storage_manager.add_table("uncompressed", load_table("resources/test_data/tbl/int_float2.tbl", 10u));
  }

  std::shared_ptr<ChunkPruningRule> _rule;
};

TEST_F(ChunkPruningTest, SimplePruningTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("compressed");

  auto predicate_node =
      std::make_shared<PredicateNode>(greater_than_(LQPColumnReference(stored_table_node, ColumnID{0}), 200));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{1}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningTest, SimpleChinkPruningTestWithColumnPruning) {
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

TEST_F(ChunkPruningTest, BetweenPruningTest) {
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

TEST_F(ChunkPruningTest, NoStatisticsAvailable) {
  auto table = StorageManager::get().get_table("uncompressed");
  auto chunk = table->get_chunk(ChunkID(0));
  EXPECT_FALSE(chunk->statistics());

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

TEST_F(ChunkPruningTest, TwoOperatorPruningTest) {
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

TEST_F(ChunkPruningTest, IntersectionPruningTest) {
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

TEST_F(ChunkPruningTest, ComparatorEdgeCasePruningTest_GreaterThan) {
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

TEST_F(ChunkPruningTest, ComparatorEdgeCasePruningTest_Equals) {
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

TEST_F(ChunkPruningTest, RangeFilterTest) {
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

TEST_F(ChunkPruningTest, LotsOfRangesFilterTest) {
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

TEST_F(ChunkPruningTest, RunLengthSegmentPruningTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("run_length_compressed");

  auto predicate_node = std::make_shared<PredicateNode>(equals_(LQPColumnReference(stored_table_node, ColumnID{0}), 2));
  predicate_node->set_left_input(stored_table_node);

  auto pruned = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(pruned, predicate_node);
  std::vector<ChunkID> expected_chunk_ids = {ChunkID{0}};
  std::vector<ChunkID> pruned_chunk_ids = stored_table_node->pruned_chunk_ids();
  EXPECT_EQ(pruned_chunk_ids, expected_chunk_ids);
}

TEST_F(ChunkPruningTest, GetTablePruningTest) {
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
  EXPECT_EQ(result_table->get_value<int>(ColumnID{0}, 0), 12345);
}

TEST_F(ChunkPruningTest, StringPruningTest) {
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

TEST_F(ChunkPruningTest, FixedStringPruningTest) {
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

TEST_F(ChunkPruningTest, PrunePastNonFilteringNodes) {
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

TEST_F(ChunkPruningTest, ValueOutOfRange) {
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
