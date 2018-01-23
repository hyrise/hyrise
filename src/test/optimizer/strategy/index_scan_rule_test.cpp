#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "abstract_expression.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/column_statistics.hpp"
#include "optimizer/strategy/index_scan_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"

#include "utils/assert.hpp"

#include "logical_query_plan/mock_node.hpp"

namespace opossum {

class TableStatisticsMock : public TableStatistics {
 public:
  // we don't need a shared_ptr<Table> for this mock, so just set a nullptr
  TableStatisticsMock() : TableStatistics(std::make_shared<Table>()) { _row_count = 0; }

  explicit TableStatisticsMock(float row_count) : TableStatistics(std::make_shared<Table>()) { _row_count = row_count; }

  std::shared_ptr<TableStatistics> predicate_statistics(const ColumnID column_id, const ScanType scan_type,
                                                        const AllParameterVariant& value,
                                                        const std::optional<AllTypeVariant>& value2) override {
    if (column_id == ColumnID{0}) {
      return std::make_shared<TableStatisticsMock>(500);
    }
    if (column_id == ColumnID{1}) {
      return std::make_shared<TableStatisticsMock>(200);
    }
    if (column_id == ColumnID{2}) {
      return std::make_shared<TableStatisticsMock>(950);
    }

    Fail("Tried to access TableStatisticsMock with unexpected column");
  }
};

class IndexScanRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("a", load_table("src/test/tables/int_int_int.tbl", Chunk::MAX_SIZE));
    _rule = std::make_shared<IndexScanRule>();
  }

  std::shared_ptr<IndexScanRule> _rule;
};

TEST_F(IndexScanRuleTest, NoIndexScanWithoutIndex) {
  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  auto statistics_mock = std::make_shared<TableStatisticsMock>();
  stored_table_node->set_statistics(statistics_mock);

  auto predicate_node_0 =
      std::make_shared<PredicateNode>(LQPColumnReference{stored_table_node, ColumnID{0}}, ScanType::GreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  EXPECT_EQ(predicate_node_0->scan_typee(), ScanTypee::TableScan);
  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_0);
  EXPECT_EQ(predicate_node_0->scan_typee(), ScanTypee::TableScan);
}

}  // namespace opossum
