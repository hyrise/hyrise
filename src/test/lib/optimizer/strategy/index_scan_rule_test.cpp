#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/index_scan_rule.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "strategy_base_test.hpp"
#include "utils/assert.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class IndexScanRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    table = load_table("resources/test_data/tbl/int_float4.tbl");
    Hyrise::get().storage_manager.add_table("a", table);
    ChunkEncoder::encode_all_chunks(Hyrise::get().storage_manager.get_table("a"));

    rule = std::make_shared<IndexScanRule>();

    stored_table_node = StoredTableNode::make("a");
    a = stored_table_node->get_column("a");
    b = stored_table_node->get_column("b");
  }

  void generate_mock_statistics(float row_count = 10.0f) {
    auto column_statistics = std::vector<std::shared_ptr<const BaseAttributeStatistics>>(2);
    const auto statistics_a = std::make_shared<AttributeStatistics<int32_t>>();
    statistics_a->set_statistics_object(GenericHistogram<int32_t>::with_single_bin(0, 20, row_count, 10));
    column_statistics[0] = statistics_a;
    const auto statistics_b = std::make_shared<AttributeStatistics<float>>();
    statistics_b->set_statistics_object(GenericHistogram<float>::with_single_bin(0, 20, row_count, 10));
    column_statistics[1] = statistics_b;

    const auto table_statistics = std::make_shared<TableStatistics>(std::move(column_statistics), row_count);
    table->set_table_statistics(table_statistics);
  }

  std::shared_ptr<IndexScanRule> rule;
  std::shared_ptr<StoredTableNode> stored_table_node;
  std::shared_ptr<Table> table;
  std::shared_ptr<LQPColumnExpression> a;
  std::shared_ptr<LQPColumnExpression> b;
};

TEST_F(IndexScanRuleTest, NoIndexScanWithoutIndex) {
  generate_mock_statistics();

  _lqp = PredicateNode::make(equals_(a, 10));
  _lqp->set_left_input(stored_table_node);

  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
  _apply_rule(rule, _lqp);
  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
}

TEST_F(IndexScanRuleTest, NoIndexScanWithIndexOnOtherColumn) {
  auto chunk_ids = std::vector<ChunkID>(table->chunk_count());
  std::iota(chunk_ids.begin(), chunk_ids.end(), 0);
  table->create_partial_hash_index(ColumnID{1}, chunk_ids);

  generate_mock_statistics();

  _lqp = PredicateNode::make(equals_(a, 10));
  _lqp->set_left_input(stored_table_node);

  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
  _apply_rule(rule, _lqp);
  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
}

TEST_F(IndexScanRuleTest, NoIndexScanWithTwoColumnPredicate) {
  generate_mock_statistics();

  _lqp = PredicateNode::make(equals_(b, b));
  _lqp->set_left_input(stored_table_node);

  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
  _apply_rule(rule, _lqp);
  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
}

TEST_F(IndexScanRuleTest, NoIndexScanWithHighSelectivity) {
  auto chunk_ids = std::vector<ChunkID>(table->chunk_count());
  std::iota(chunk_ids.begin(), chunk_ids.end(), 0);
  table->create_partial_hash_index(ColumnID{1}, chunk_ids);

  generate_mock_statistics(80'000);

  _lqp = PredicateNode::make(not_equals_(b, 10));
  _lqp->set_left_input(stored_table_node);

  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
  _apply_rule(rule, _lqp);
  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
}

TEST_F(IndexScanRuleTest, IndexScanWithIndex) {
  auto chunk_ids = std::vector<ChunkID>(table->chunk_count());
  std::iota(chunk_ids.begin(), chunk_ids.end(), 0);
  table->create_partial_hash_index(ColumnID{1}, chunk_ids);

  generate_mock_statistics(1'000'000);

  _lqp = PredicateNode::make(equals_(b, 19'900));
  _lqp->set_left_input(stored_table_node);

  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
  _apply_rule(rule, _lqp);
  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::IndexScan);
}

TEST_F(IndexScanRuleTest, IndexScanWithIndexPrunedColumn) {
  auto chunk_ids = std::vector<ChunkID>(table->chunk_count());
  std::iota(chunk_ids.begin(), chunk_ids.end(), 0);
  table->create_partial_hash_index(ColumnID{1}, chunk_ids);

  stored_table_node->set_pruned_column_ids({ColumnID{0}});

  generate_mock_statistics(1'000'000);

  _lqp = PredicateNode::make(equals_(b, 19'900));
  _lqp->set_left_input(stored_table_node);

  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
  _apply_rule(rule, _lqp);
  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::IndexScan);
}

TEST_F(IndexScanRuleTest, IndexScanOnlyOnOutputOfStoredTableNode) {
  auto chunk_ids = std::vector<ChunkID>(table->chunk_count());
  std::iota(chunk_ids.begin(), chunk_ids.end(), 0);
  table->create_partial_hash_index(ColumnID{1}, chunk_ids);

  generate_mock_statistics(1'000'000);

  _lqp = PredicateNode::make(less_than_(b, 15));
  const auto predicate_node = PredicateNode::make(equals_(b, 19'900));
  _lqp->set_left_input(predicate_node);
  predicate_node->set_left_input(stored_table_node);

  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
  EXPECT_EQ(predicate_node->scan_type, ScanType::TableScan);

  _apply_rule(rule, _lqp);

  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
  EXPECT_EQ(predicate_node->scan_type, ScanType::IndexScan);
}

// Same test as before, but placing the predicate with a high selectivity first, which does not trigger an index
// scan. The second predicate has a very low selectivity, but does not follow a stored table node.
TEST_F(IndexScanRuleTest, NoIndexScanForSecondPredicate) {
  auto chunk_ids = std::vector<ChunkID>(table->chunk_count());
  std::iota(chunk_ids.begin(), chunk_ids.end(), 0);
  table->create_partial_hash_index(ColumnID{1}, chunk_ids);

  generate_mock_statistics(1'000'000);

  _lqp = PredicateNode::make(equals_(b, 19'900));
  const auto predicate_node = PredicateNode::make(less_than_(b, 15));
  _lqp->set_left_input(predicate_node);
  predicate_node->set_left_input(stored_table_node);

  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
  EXPECT_EQ(predicate_node->scan_type, ScanType::TableScan);

  _apply_rule(rule, _lqp);

  EXPECT_EQ(static_cast<PredicateNode&>(*_lqp).scan_type, ScanType::TableScan);
  EXPECT_EQ(predicate_node->scan_type, ScanType::TableScan);
}

}  // namespace hyrise
