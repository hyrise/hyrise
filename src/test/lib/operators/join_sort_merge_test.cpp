#include "base_test.hpp"

#include "operators/join_sort_merge.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

class OperatorsJoinSortMergeTest : public BaseTest {
 public:
  void SetUp() override {
    const auto dummy_table =
        std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
    dummy_input = std::make_shared<TableWrapper>(dummy_table);
  }

  std::shared_ptr<AbstractOperator> dummy_input;
};

TEST_F(OperatorsJoinSortMergeTest, DescriptionAndName) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto secondary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::NotEquals};

  const auto join_operator =
      std::make_shared<JoinSortMerge>(dummy_input, dummy_input, JoinMode::Inner, primary_predicate,
                                      std::vector<OperatorJoinPredicate>{secondary_predicate});

  EXPECT_EQ(join_operator->description(DescriptionMode::SingleLine),
            "JoinSortMerge (Inner Join where Column #0 = Column #0 AND Column #0 != Column #0)");
  EXPECT_EQ(join_operator->description(DescriptionMode::MultiLine),
            "JoinSortMerge\n(Inner Join where Column #0 = Column #0 AND Column #0 != Column #0)");

  dummy_input->execute();
  EXPECT_EQ(join_operator->description(DescriptionMode::SingleLine),
            "JoinSortMerge (Inner Join where a = a AND a != a)");
  EXPECT_EQ(join_operator->description(DescriptionMode::MultiLine),
            "JoinSortMerge\n(Inner Join where a = a AND a != a)");

  EXPECT_EQ(join_operator->name(), "JoinSortMerge");
}

TEST_F(OperatorsJoinSortMergeTest, DeepCopy) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto join_operator =
      std::make_shared<JoinSortMerge>(dummy_input, dummy_input, JoinMode::Left, primary_predicate);
  const auto abstract_join_operator_copy = join_operator->deep_copy();
  const auto join_operator_copy = std::dynamic_pointer_cast<JoinSortMerge>(join_operator);

  ASSERT_TRUE(join_operator_copy);

  EXPECT_EQ(join_operator_copy->mode(), JoinMode::Left);
  EXPECT_EQ(join_operator_copy->primary_predicate(), primary_predicate);
  EXPECT_NE(join_operator_copy->left_input(), nullptr);
  EXPECT_NE(join_operator_copy->right_input(), nullptr);
}

TEST_F(OperatorsJoinSortMergeTest, ValueClusteringFlag) {
  const auto test_table = std::make_shared<Table>(
      TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::Int, false}},
      TableType::Data);

  test_table->append({1, 2, 3});
  test_table->append({2, 1, 4});
  test_table->append({1, 2, 5});

  const auto test_input = std::make_shared<TableWrapper>(test_table);
  test_input->execute();
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals};

  // For inner joins, both join columns are clustered
  {
    const auto join_operator =
        std::make_shared<JoinSortMerge>(test_input, test_input, JoinMode::Inner, primary_predicate);
    join_operator->execute();

    const std::vector<ColumnID> expected_value_clustering{ColumnID{0}, ColumnID{4}};
    const auto& actual_value_clustering = join_operator->get_output()->value_clustered_by();
    EXPECT_EQ(actual_value_clustering, expected_value_clustering);
  }

  // For outer joins, the table cannot be clustered as table clustering is not defined for NULL values
  {
    const auto join_operator =
        std::make_shared<JoinSortMerge>(test_input, test_input, JoinMode::Left, primary_predicate);
    join_operator->execute();

    const auto& actual_value_clustering = join_operator->get_output()->value_clustered_by();
    EXPECT_TRUE(actual_value_clustering.empty());
  }
}

TEST_F(OperatorsJoinSortMergeTest, SetSortedFlagOnJoinColumns) {
  const auto test_table = std::make_shared<Table>(
      TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::Int, false}},
      TableType::Data);

  test_table->append({1, 2, 3});
  test_table->append({2, 1, 4});
  test_table->append({1, 2, 5});

  const auto test_input = std::make_shared<TableWrapper>(test_table);

  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals};
  const auto join_operator =
      std::make_shared<JoinSortMerge>(test_input, test_input, JoinMode::Inner, primary_predicate);

  test_input->execute();
  join_operator->execute();

  const auto& output_table = join_operator->get_output();

  const auto expected_sorted_columns = std::vector<SortColumnDefinition>{
      SortColumnDefinition(ColumnID{0}, SortMode::Ascending), SortColumnDefinition(ColumnID{4}, SortMode::Ascending)};
  for (auto chunk_id = ChunkID{0}; chunk_id < output_table->chunk_count(); ++chunk_id) {
    const auto& actual_sorted_columns = output_table->get_chunk(chunk_id)->individually_sorted_by();
    EXPECT_EQ(actual_sorted_columns, expected_sorted_columns);
  }
}

}  // namespace opossum
