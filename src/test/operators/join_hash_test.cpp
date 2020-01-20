#include "../base_test.hpp"

#include "operators/join_hash.hpp"
#include "operators/table_wrapper.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsJoinHashTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table_wrapper_small =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/join_operators/anti_int4.tbl", 2));
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

  void SetUp() override {
    const auto dummy_table =
        std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
    dummy_input = std::make_shared<TableWrapper>(dummy_table);
  }

  std::shared_ptr<AbstractOperator> dummy_input;
  inline static std::shared_ptr<TableWrapper> _table_wrapper_small, _table_tpch_orders, _table_tpch_lineitems,
      _table_with_nulls;
  inline static std::shared_ptr<TableScan> _table_tpch_orders_scanned, _table_tpch_lineitems_scanned;
};

TEST_F(OperatorsJoinHashTest, OperatorName) {
  auto join = std::make_shared<JoinHash>(
      _table_wrapper_small, _table_wrapper_small, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});

  EXPECT_EQ(join->name(), "JoinHash");
}

// Once we bring in the PosList optimization flag REFERS_TO_SINGLE_CHUNK_ONLY, this test will ensure
// that the join does not unnecessarily add chunks (e.g., discussed in #698).
TEST_F(OperatorsJoinHashTest, DISABLED_ChunkCount /* #698 */) {
  auto join = std::make_shared<JoinHash>(_table_tpch_orders_scanned, _table_tpch_lineitems_scanned, JoinMode::Inner,
                                         OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                         std::vector<OperatorJoinPredicate>{}, 10);
  join->execute();

  // While radix clustering is well-suited for very large tables, it also yields many output tables.
  // This test checks whether we create more chunks that existing in the input (which should not be the case).
  EXPECT_TRUE(join->get_output()->chunk_count() <=
              std::max(_table_tpch_orders_scanned->get_output()->chunk_count(),
                       _table_tpch_lineitems_scanned->get_output()->chunk_count()));
}

TEST_F(OperatorsJoinHashTest, DescriptionAndName) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto secondary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::NotEquals};

  const auto join_operator = std::make_shared<JoinHash>(dummy_input, dummy_input, JoinMode::Inner, primary_predicate,
                                                        std::vector<OperatorJoinPredicate>{secondary_predicate});
  const auto join_operator_with_radix =
      std::make_shared<JoinHash>(dummy_input, dummy_input, JoinMode::Inner, primary_predicate,
                                 std::vector<OperatorJoinPredicate>{secondary_predicate}, 4);

  EXPECT_EQ(join_operator->description(DescriptionMode::SingleLine),
            "JoinHash (Inner Join where Column #0 = Column #0 AND Column #0 != Column #0) Radix bits: Unspecified");
  EXPECT_EQ(join_operator->description(DescriptionMode::MultiLine),
            "JoinHash\n(Inner Join where Column #0 = Column #0 AND Column #0 != Column #0) Radix bits: Unspecified");
  EXPECT_EQ(join_operator_with_radix->description(DescriptionMode::MultiLine),
            "JoinHash\n(Inner Join where Column #0 = Column #0 AND Column #0 != Column #0) Radix bits: 4");

  dummy_input->execute();
  EXPECT_EQ(join_operator->description(DescriptionMode::SingleLine),
            "JoinHash (Inner Join where a = a AND a != a) Radix bits: Unspecified");
  EXPECT_EQ(join_operator->description(DescriptionMode::MultiLine),
            "JoinHash\n(Inner Join where a = a AND a != a) Radix bits: Unspecified");

  EXPECT_EQ(join_operator->name(), "JoinHash");
}

TEST_F(OperatorsJoinHashTest, DeepCopy) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto join_operator = std::make_shared<JoinHash>(dummy_input, dummy_input, JoinMode::Left, primary_predicate);
  const auto abstract_join_operator_copy = join_operator->deep_copy();
  const auto join_operator_copy = std::dynamic_pointer_cast<JoinHash>(join_operator);

  ASSERT_TRUE(join_operator_copy);

  EXPECT_EQ(join_operator_copy->mode(), JoinMode::Left);
  EXPECT_EQ(join_operator_copy->primary_predicate(), primary_predicate);
  EXPECT_NE(join_operator_copy->input_left(), nullptr);
  EXPECT_NE(join_operator_copy->input_right(), nullptr);
}

TEST_F(OperatorsJoinHashTest, RadixBitCalculation) {
  // Simple cases: handle minimal inputs and very large inputs
  EXPECT_EQ(JoinHash::calculate_radix_bits<int>(1, 1), 0ul);
  EXPECT_EQ(JoinHash::calculate_radix_bits<int>(0, 1), 0ul);
  EXPECT_EQ(JoinHash::calculate_radix_bits<int>(1, 0), 0ul);
  EXPECT_TRUE(JoinHash::calculate_radix_bits<int>(std::numeric_limits<size_t>::max(),
                                                  std::numeric_limits<size_t>::max()) > 0ul);
}

}  // namespace opossum
