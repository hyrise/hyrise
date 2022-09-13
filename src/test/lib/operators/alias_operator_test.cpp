#include "base_test.hpp"

#include "operators/alias_operator.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

class AliasOperatorTest : public BaseTest {
 public:
  void SetUp() override {
    table_wrapper =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int_float.tbl", ChunkOffset{1}));
    table_wrapper->never_clear_output();
    table_wrapper->execute();

    column_ids = std::vector<ColumnID>({ColumnID{2}, ColumnID{0}, ColumnID{1}});
    aliases = std::vector<std::string>({"z", "x", "y"});

    alias_operator = std::make_shared<AliasOperator>(table_wrapper, column_ids, aliases);
    alias_operator->never_clear_output();
  }

  std::shared_ptr<AliasOperator> alias_operator;
  std::shared_ptr<TableWrapper> table_wrapper;
  std::vector<ColumnID> column_ids;
  std::vector<std::string> aliases;
};

TEST_F(AliasOperatorTest, Name) {
  EXPECT_EQ(alias_operator->name(), "Alias");
  EXPECT_EQ(alias_operator->description(DescriptionMode::SingleLine), "Alias [z, x, y]");
  EXPECT_EQ(alias_operator->description(DescriptionMode::MultiLine), "Alias\n[z, x, y]");
}

TEST_F(AliasOperatorTest, OutputColumnNames) {
  alias_operator->execute();
  EXPECT_TABLE_EQ_ORDERED(alias_operator->get_output(),
                          load_table("resources/test_data/tbl/int_int_float_aliased.tbl"));
}

TEST_F(AliasOperatorTest, ForwardSortedByFlag) {
  alias_operator->execute();
  const auto result_table_unsorted = alias_operator->get_output();

  for (ChunkID chunk_id{0}; chunk_id < result_table_unsorted->chunk_count(); ++chunk_id) {
    const auto& sorted_by = result_table_unsorted->get_chunk(chunk_id)->individually_sorted_by();
    EXPECT_TRUE(sorted_by.empty());
  }

  // Verify that the sorted_by flag is set when it's present in input.
  const auto sort_definition = std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}}};
  auto sort = std::make_shared<Sort>(table_wrapper, sort_definition);
  sort->execute();

  auto alias_operator_sorted = std::make_shared<AliasOperator>(sort, column_ids, aliases);
  alias_operator_sorted->execute();

  const auto result_table_sorted = alias_operator_sorted->get_output();
  for (ChunkID chunk_id{0}; chunk_id < result_table_sorted->chunk_count(); ++chunk_id) {
    const auto& sorted_by = result_table_sorted->get_chunk(chunk_id)->individually_sorted_by();
    ASSERT_EQ(sorted_by.size(), 1);
    EXPECT_EQ(sorted_by.front().column, ColumnID{1});
    EXPECT_EQ(sorted_by.front().sort_mode, SortMode::Ascending);
  }
}

// This test checks for an edge case where a sorted column appears twice (due to a repeated projection) but only one of
// the two columns is known to the alias operator (see #2321).
TEST_F(AliasOperatorTest, ForwardSortedByFlagForRepeatedColumnReferences) {
  const auto sort_definition = std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}}};
  const auto sort = std::make_shared<Sort>(table_wrapper, sort_definition);
  sort->execute();

  const auto col_a = PQPColumnExpression::from_table(*sort->get_output(), "a");
  const auto col_b = PQPColumnExpression::from_table(*sort->get_output(), "b");
  const auto projection = std::make_shared<Projection>(sort, expression_vector(col_b, col_a, col_a));
  projection->execute();

  const auto result_table_projection = projection->get_output();
  for (auto chunk_id = ChunkID{0}; chunk_id < result_table_projection->chunk_count(); ++chunk_id) {
    const auto& chunk_sorted_by = result_table_projection->get_chunk(chunk_id)->individually_sorted_by();
    const auto expected_sorted_by =
        std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{1}}, SortColumnDefinition{ColumnID{2}}};

    // We directly check for vector equality as an unordered_map is used in the projection
    for (const auto& sort_column : expected_sorted_by) {
      const auto iter = std::find(chunk_sorted_by.begin(), chunk_sorted_by.end(), sort_column);
      EXPECT_TRUE(iter != chunk_sorted_by.end());
    }
  }

  const auto alias =
      std::make_shared<AliasOperator>(projection, std::vector<ColumnID>{{ColumnID{0}, ColumnID{1}, ColumnID{1}}},
                                      std::vector<std::string>{{"col_b", "col_a1", "col_a2"}});
  alias->execute();

  const auto result_table_alias = alias->get_output();
  for (auto chunk_id = ChunkID{0}; chunk_id < result_table_alias->chunk_count(); ++chunk_id) {
    const auto& chunk_sorted_by = result_table_alias->get_chunk(chunk_id)->individually_sorted_by();
    const auto expected_sorted_by =
        std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{1}}, SortColumnDefinition{ColumnID{2}}};

    ASSERT_EQ(expected_sorted_by, chunk_sorted_by);
  }
}

}  // namespace hyrise
