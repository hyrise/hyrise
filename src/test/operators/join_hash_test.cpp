#include "../base_test.hpp"

#include "operators/join_hash.hpp"
#include "operators/table_wrapper.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for the JoinHash implementation.
*/

class JoinHashTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table_wrapper_small = std::make_shared<TableWrapper>(load_table("src/test/tables/joinoperators/anti_int4.tbl", 2));
    _table_wrapper_small->execute();

    _table_tpch_orders = std::make_shared<TableWrapper>(load_table("src/test/tables/tpch/sf-0.001/orders.tbl", 10));
    _table_tpch_orders->execute();

    _table_tpch_lineitems =
        std::make_shared<TableWrapper>(load_table("src/test/tables/tpch/sf-0.001/lineitem.tbl", 10));
    _table_tpch_lineitems->execute();

    _table_with_nulls = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int4_with_null.tbl", 10));
    _table_with_nulls->execute();

    // filters retain all rows
    _table_tpch_orders_scanned = create_table_scan(_table_tpch_orders, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_orders_scanned->execute();
    _table_tpch_lineitems_scanned =
        create_table_scan(_table_tpch_lineitems, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_lineitems_scanned->execute();
  }

  void SetUp() override {}

  inline static std::shared_ptr<TableWrapper> _table_wrapper_small, _table_tpch_orders, _table_tpch_lineitems,
      _table_with_nulls;
  inline static std::shared_ptr<TableScan> _table_tpch_orders_scanned, _table_tpch_lineitems_scanned;
};

TEST_F(JoinHashTest, OperatorName) {
  auto join = std::make_shared<JoinHash>(_table_wrapper_small, _table_wrapper_small, JoinMode::Inner,
                                         ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);

  EXPECT_EQ(join->name(), "JoinHash");
}

// Once we bring in the PosList optimization flag REFERS_TO_SINGLE_CHUNK_ONLY, this test will ensure
// that the join does not unnecessarily add chunks (e.g., discussed in #698).
TEST_F(JoinHashTest, DISABLED_ChunkCount /* #698 */) {
  auto join = std::make_shared<JoinHash>(_table_tpch_orders_scanned, _table_tpch_lineitems_scanned, JoinMode::Inner,
                                         ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals, 10);
  join->execute();

  // While radix clustering is well-suited for very large tables, it also yield many output tables.
  // This test checks whether we create more chunks that existing in the input (which should not be the case).
  EXPECT_TRUE(join->get_output()->chunk_count() <=
              std::max(_table_tpch_orders_scanned->get_output()->chunk_count(),
                       _table_tpch_lineitems_scanned->get_output()->chunk_count()));
}

TEST_F(JoinHashTest, RadixClusteredLeftJoinWithZeroAndOnesAnd) {
  // This test mirrors the test "LeftJoinWithNullAndZeros" executed in join_null_test, but for such input
  // sizes no radix clustering is executed. Consequently, we manually create a radix clustered hash join.
  auto join = std::make_shared<JoinHash>(_table_with_nulls, _table_with_nulls, JoinMode::Left,
                                         ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals, 2);
  join->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/joinoperators/int_with_null_and_zero.tbl", 1);
  EXPECT_TABLE_EQ_UNORDERED(join->get_output(), expected_result);
}

}  // namespace opossum
