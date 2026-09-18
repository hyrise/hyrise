#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"
#include "testing_assert.hpp"
#include "storage/table_column_definition.hpp"

namespace hyrise {

class OperatorsJoinHashTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table_wrapper_small = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/join_operators/anti_int4.tbl", ChunkOffset{2}));
    _table_wrapper_small->execute();

    _table_tpch_orders =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/tpch/sf-0.001/orders.tbl", ChunkOffset{10}));
    _table_tpch_orders->execute();

    _table_tpch_lineitems = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/tpch/sf-0.001/lineitem.tbl", ChunkOffset{10}));
    _table_tpch_lineitems->execute();

    _table_with_nulls =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int4_with_null.tbl", ChunkOffset{10}));
    _table_with_nulls->execute();

    // Filters retain all rows.
    _table_tpch_orders_scanned = create_table_scan(_table_tpch_orders, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_orders_scanned->never_clear_output();
    _table_tpch_orders_scanned->execute();

    _table_tpch_lineitems_scanned =
        create_table_scan(_table_tpch_lineitems, ColumnID{0}, PredicateCondition::GreaterThan, 0);
    _table_tpch_lineitems_scanned->never_clear_output();
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

// This test ensures that the join does not unnecessarily add chunks (e.g., discussed in #698).
TEST_F(OperatorsJoinHashTest, ChunkCount) {
  auto join = std::make_shared<JoinHash>(_table_tpch_orders_scanned, _table_tpch_lineitems_scanned, JoinMode::Inner,
                                         OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                                         std::vector<OperatorJoinPredicate>{}, 10);
  join->never_clear_output();
  join->execute();

  // While radix clustering is well-suited for very large tables, it also yields many output chunks (one per radix
  // partition). This test checks whether we create more chunks that existing in the input.
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
            "JoinHash (Inner) Column #0 = Column #0 AND Column #0 != Column #0");
  EXPECT_EQ(join_operator->description(DescriptionMode::MultiLine),
            "JoinHash (Inner)\nColumn #0 = Column #0\nAND Column #0 != Column #0");
  EXPECT_EQ(join_operator_with_radix->description(DescriptionMode::MultiLine),
            "JoinHash (Inner)\nColumn #0 = Column #0\nAND Column #0 != Column #0");

  dummy_input->execute();
  EXPECT_EQ(join_operator->description(DescriptionMode::SingleLine), "JoinHash (Inner) a = a AND a != a");
  EXPECT_EQ(join_operator->description(DescriptionMode::MultiLine), "JoinHash (Inner)\na = a\nAND a != a");

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
  EXPECT_NE(join_operator_copy->left_input(), nullptr);
  EXPECT_NE(join_operator_copy->right_input(), nullptr);
}

TEST_F(OperatorsJoinHashTest, RadixBitCalculation) {
  // Simple tests to check that side switching and zero-sizes work.
  EXPECT_EQ(JoinHash::calculate_radix_bits(1, 0), 0);
  EXPECT_EQ(JoinHash::calculate_radix_bits(0, 1), 0);
  EXPECT_EQ(JoinHash::calculate_radix_bits(0, 0), 0);
  EXPECT_EQ(JoinHash::calculate_radix_bits(1, 1), 0);
  EXPECT_GT(JoinHash::calculate_radix_bits(std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max()), 0);
}

TEST_F(OperatorsJoinHashTest, SemiJoinBuildsOnSmallerSide) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};

  // Precondition of this test: the two inputs differ clearly in size.
  ASSERT_LT(_table_tpch_orders_scanned->get_output()->row_count(),
            _table_tpch_lineitems_scanned->get_output()->row_count());

  // The left input is the smaller one. It becomes the build side and the emitted rows are gathered from the hash
  // table after probing.
  const auto join_small_left = std::make_shared<JoinHash>(_table_tpch_orders_scanned, _table_tpch_lineitems_scanned,
                                                          JoinMode::Semi, primary_predicate);
  join_small_left->execute();
  const auto& performance_data_small_left =
      static_cast<const JoinHash::PerformanceData&>(*join_small_left->performance_data);
  EXPECT_TRUE(performance_data_small_left.left_input_is_build_side);

  // The left input is the larger one. The previous behavior is retained: the right input becomes the build side and
  // the emitted rows are those of the probe side.
  const auto join_small_right = std::make_shared<JoinHash>(_table_tpch_lineitems_scanned, _table_tpch_orders_scanned,
                                                           JoinMode::Semi, primary_predicate);
  join_small_right->execute();
  const auto& performance_data_small_right =
      static_cast<const JoinHash::PerformanceData&>(*join_small_right->performance_data);
  EXPECT_FALSE(performance_data_small_right.left_input_is_build_side);
}

TEST_F(OperatorsJoinHashTest, SemiJoinEmittingFromBuildSide) {
  // The left input is smaller, so the hash table is built on the left side. Duplicates on the build side must each be
  // emitted once, while duplicates on the probe side must not multiply the output.
  const auto column_definitions = TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Int, false}};
  const auto left_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});
  left_table->append({1, 10});
  left_table->append({1, 11});
  left_table->append({2, 12});
  left_table->append({3, 13});
  left_table->append({3, 14});

  const auto right_table =
      std::make_shared<Table>(TableColumnDefinitions{{"c", DataType::Int, false}}, TableType::Data, ChunkOffset{3});
  for (const auto value : {1, 1, 2, 2, 2, 5, 5, 6}) {
    right_table->append({value});
  }

  const auto left_input = std::make_shared<TableWrapper>(left_table);
  left_input->never_clear_output();
  left_input->execute();
  const auto right_input = std::make_shared<TableWrapper>(right_table);
  right_input->never_clear_output();
  right_input->execute();

  ASSERT_LT(left_input->get_output()->row_count(), right_input->get_output()->row_count());

  // Both rows with a == 1 are emitted, the single row with a == 2 is emitted once despite three matches on the probe
  // side, and the rows with a == 3 have no match at all.
  const auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});
  expected_table->append({1, 10});
  expected_table->append({1, 11});
  expected_table->append({2, 12});

  const auto join = std::make_shared<JoinHash>(
      left_input, right_input, JoinMode::Semi,
      OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->never_clear_output();
  join->execute();

  const auto& performance_data = static_cast<const JoinHash::PerformanceData&>(*join->performance_data);
  EXPECT_TRUE(performance_data.left_input_is_build_side);

  EXPECT_TABLE_EQ_UNORDERED(join->get_output(), expected_table);
}

}  // namespace hyrise
