#include "../base_test.hpp"

#include "operators/join_proxy.hpp"
#include "operators/table_wrapper.hpp"
#include "types.hpp"

namespace opossum {

// TODO(Sven): Integrate tests with join test runner

class DISABLED_JoinProxyTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table_wrapper_small =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/joinoperators/anti_int4.tbl", 2));
    _table_wrapper_small->execute();

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

  void SetUp() override {}

  inline static std::shared_ptr<TableWrapper> _table_wrapper_small, _table_tpch_orders, _table_tpch_lineitems,
      _table_with_nulls;
  inline static std::shared_ptr<TableScan> _table_tpch_orders_scanned, _table_tpch_lineitems_scanned;
};

TEST_F(DISABLED_JoinProxyTest, OperatorName) {
  auto join = std::make_shared<JoinProxy>(_table_wrapper_small, _table_wrapper_small, JoinMode::Inner,
                                          OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});

  EXPECT_EQ(join->name(), "JoinProxy");
}

TEST_F(DISABLED_JoinProxyTest, RadixClusteredLeftJoinWithZeroAndOnesAnd) {
  auto join = std::make_shared<JoinProxy>(_table_with_nulls, _table_with_nulls, JoinMode::Left,
                                          OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
  join->execute();

  std::shared_ptr<Table> expected_result =
      load_table("resources/test_data/tbl/joinoperators/int_with_null_and_zero.tbl", 1);
  EXPECT_TABLE_EQ_UNORDERED(join->get_output(), expected_result);
}

}  // namespace opossum
