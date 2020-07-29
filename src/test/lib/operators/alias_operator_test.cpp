#include "base_test.hpp"

#include "operators/alias_operator.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class AliasOperatorTest : public BaseTest {
 public:
  void SetUp() override {
    table_wrapper = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int_float.tbl", 1));
    table_wrapper->execute();

    column_ids = std::vector<ColumnID>({ColumnID{2}, ColumnID{0}, ColumnID{1}});
    aliases = std::vector<std::string>({"z", "x", "y"});

    alias_operator = std::make_shared<AliasOperator>(table_wrapper, column_ids, aliases);
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
  const auto sort_definition =
      std::vector<SortColumnDefinition>{SortColumnDefinition(ColumnID{0}, SortMode::Ascending)};
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

}  // namespace opossum
