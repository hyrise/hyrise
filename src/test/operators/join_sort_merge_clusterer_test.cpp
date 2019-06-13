#include "../base_test.hpp"

#include "operators/join_hash.hpp"
#include "operators/table_wrapper.hpp"
#include "types.hpp"

namespace opossum {

class JoinSortMergeClustererTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table_tpch_orders =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/tpch/sf-0.001/orders.tbl", 10));
    _table_tpch_orders->execute();

    _table_tpch_lineitems =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/tpch/sf-0.001/lineitem.tbl", 10));
    _table_tpch_lineitems->execute();

    _table_with_nulls =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int4_with_null.tbl", 10));
    _table_with_nulls->execute();

    // filters retain all rows
    _table_tpch_orders_scanned = create_table_scan(_table_tpch_orders, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_orders_scanned->execute();
    _table_tpch_lineitems_scanned =
        create_table_scan(_table_tpch_lineitems, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_lineitems_scanned->execute();
  }

  void SetUp() override {
    const auto dummy_table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int}}, TableType::Data);
    dummy_input = std::make_shared<TableWrapper>(dummy_table);
  }

  std::shared_ptr<AbstractOperator> dummy_input;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_small, _table_tpch_orders, _table_tpch_lineitems,
      _table_with_nulls;
  inline static std::shared_ptr<TableScan> _table_tpch_orders_scanned, _table_tpch_lineitems_scanned;
};

TEST_F(JoinSortMergeClustererTest, RangeClustering) {
  EXPECT_TRUE(false);
}

TEST_F(JoinSortMergeClustererTest, RadixClustering) {
  EXPECT_TRUE(false);
}

TEST_F(JoinSortMergeClustererTest, ConcatenateForSingleCluster) {
  EXPECT_TRUE(false);
}

TEST_F(JoinSortMergeClustererTest, SortPartiallySortedSegments) {
  EXPECT_TRUE(false);

	// DebugAssert(std::is_sorted(materialized_segment.begin(), materialized_segment.end(),
	//                            [](auto& left, auto& right) { return left.value < right.value; }),
	//             "Resulting materializied segment is unsorted.");
}

}  // namespace opossum
