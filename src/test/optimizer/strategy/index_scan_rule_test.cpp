#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "utils/assert.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/index_scan_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class IndexScanRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    table = load_table("resources/test_data/tbl/int_int_int.tbl");
    Hyrise::get().storage_manager.add_table("a", table);
    ChunkEncoder::encode_all_chunks(Hyrise::get().storage_manager.get_table("a"));

    rule = std::make_shared<IndexScanRule>();

    stored_table_node = StoredTableNode::make("a");
    a = stored_table_node->get_column("a");
    b = stored_table_node->get_column("b");
    c = stored_table_node->get_column("c");
  }

  void generate_mock_statistics(float row_count = 10.0f) {
    const auto table_statistics = table->table_statistics();
    table_statistics->row_count = row_count;

    table_statistics->column_statistics.at(0)->set_statistics_object(
        GenericHistogram<int32_t>::with_single_bin(0, 20, row_count, 10));
    table_statistics->column_statistics.at(1)->set_statistics_object(
        GenericHistogram<int32_t>::with_single_bin(0, 20, row_count, 10));
    table_statistics->column_statistics.at(2)->set_statistics_object(
        GenericHistogram<int32_t>::with_single_bin(0, 20'000, row_count, 10));
  }

  std::shared_ptr<IndexScanRule> rule;
  std::shared_ptr<StoredTableNode> stored_table_node;
  std::shared_ptr<Table> table;
  LQPColumnReference a, b, c;
};

TEST_F(IndexScanRuleTest, NoIndexScanWithoutIndex) {
  generate_mock_statistics();

  auto predicate_node_0 = PredicateNode::make(greater_than_(a, 10));
  predicate_node_0->set_left_input(stored_table_node);

  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
  auto reordered = StrategyBaseTest::apply_rule(rule, predicate_node_0);
  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
}

TEST_F(IndexScanRuleTest, NoIndexScanWithIndexOnOtherColumn) {
  table->create_index<GroupKeyIndex>({ColumnID{2}});

  generate_mock_statistics();

  auto predicate_node_0 = PredicateNode::make(greater_than_(a, 10));
  predicate_node_0->set_left_input(stored_table_node);

  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
  auto reordered = StrategyBaseTest::apply_rule(rule, predicate_node_0);
  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
}

TEST_F(IndexScanRuleTest, NoIndexScanWithMultiSegmentIndex) {
  table->create_index<CompositeGroupKeyIndex>({ColumnID{2}, ColumnID{1}});

  generate_mock_statistics();

  auto predicate_node_0 = PredicateNode::make(greater_than_(c, 10));
  predicate_node_0->set_left_input(stored_table_node);

  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
  auto reordered = StrategyBaseTest::apply_rule(rule, predicate_node_0);
  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
}

TEST_F(IndexScanRuleTest, NoIndexScanWithTwoColumnPredicate) {
  generate_mock_statistics();

  auto predicate_node_0 = PredicateNode::make(greater_than_(c, b));
  predicate_node_0->set_left_input(stored_table_node);

  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
  auto reordered = StrategyBaseTest::apply_rule(rule, predicate_node_0);
  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
}

TEST_F(IndexScanRuleTest, NoIndexScanWithHighSelectivity) {
  table->create_index<GroupKeyIndex>({ColumnID{2}});

  generate_mock_statistics(80'000);

  auto predicate_node_0 = PredicateNode::make(greater_than_(c, 10));
  predicate_node_0->set_left_input(stored_table_node);

  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
  auto reordered = StrategyBaseTest::apply_rule(rule, predicate_node_0);
  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
}

TEST_F(IndexScanRuleTest, NoIndexScanIfNotGroupKey) {
  table->create_index<AdaptiveRadixTreeIndex>({ColumnID{2}});

  generate_mock_statistics(1'000'000);

  auto predicate_node_0 = PredicateNode::make(greater_than_(c, 10));
  predicate_node_0->set_left_input(stored_table_node);

  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
  auto reordered = StrategyBaseTest::apply_rule(rule, predicate_node_0);
  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
}

TEST_F(IndexScanRuleTest, IndexScanWithIndex) {
  table->create_index<GroupKeyIndex>({ColumnID{2}});

  generate_mock_statistics(1'000'000);

  auto predicate_node_0 = PredicateNode::make(greater_than_(c, 19'900));
  predicate_node_0->set_left_input(stored_table_node);

  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
  auto reordered = StrategyBaseTest::apply_rule(rule, predicate_node_0);
  EXPECT_EQ(predicate_node_0->scan_type, ScanType::IndexScan);
}

TEST_F(IndexScanRuleTest, IndexScanWithIndexPrunedColumn) {
  table->create_index<GroupKeyIndex>({ColumnID{2}});
  stored_table_node->set_pruned_column_ids({ColumnID{0}});

  generate_mock_statistics(1'000'000);

  auto predicate_node_0 = PredicateNode::make(greater_than_(c, 19'900));
  predicate_node_0->set_left_input(stored_table_node);

  EXPECT_EQ(predicate_node_0->scan_type, ScanType::TableScan);
  auto reordered = StrategyBaseTest::apply_rule(rule, predicate_node_0);
  EXPECT_EQ(predicate_node_0->scan_type, ScanType::IndexScan);
}

TEST_F(IndexScanRuleTest, IndexScanOnlyOnOutputOfStoredTableNode) {
  table->create_index<GroupKeyIndex>({ColumnID{2}});

  generate_mock_statistics(1'000'000);

  auto predicate_node_0 = PredicateNode::make(greater_than_(c, 19'900));
  predicate_node_0->set_left_input(stored_table_node);

  auto predicate_node_1 = PredicateNode::make(less_than_(b, 15));
  predicate_node_1->set_left_input(predicate_node_0);

  auto reordered = StrategyBaseTest::apply_rule(rule, predicate_node_1);
  EXPECT_EQ(predicate_node_0->scan_type, ScanType::IndexScan);
  EXPECT_EQ(predicate_node_1->scan_type, ScanType::TableScan);
}

}  // namespace opossum
