#include "base_test.hpp"

#include "operators/alias_operator.hpp"
#include "operators/print.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class AliasOperatorTest : public BaseTest {
 public:
  void SetUp() override {
    const auto table_wrapper =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int_float.tbl", 1));
    table_wrapper->execute();

    auto column_ids = std::vector<ColumnID>({ColumnID{2}, ColumnID{0}, ColumnID{1}});
    auto aliases = std::vector<std::string>({"z", "x", "y"});

    alias_operator = std::make_shared<AliasOperator>(table_wrapper, column_ids, aliases);
  }

  std::shared_ptr<AliasOperator> alias_operator;
};

TEST_F(AliasOperatorTest, Name) {
  EXPECT_EQ(alias_operator->name(), "Alias");
  EXPECT_EQ(alias_operator->description(DescriptionMode::SingleLine), "Alias [z, x, y]");
  EXPECT_EQ(alias_operator->description(DescriptionMode::MultiLine), "Alias\n[z\nx\ny]");
}

TEST_F(AliasOperatorTest, OutputColumnNames) {
  alias_operator->execute();
  EXPECT_TABLE_EQ_ORDERED(alias_operator->get_output(),
                          load_table("resources/test_data/tbl/int_int_float_aliased.tbl"));
}

TEST_F(AliasOperatorTest, ForwardSortFlag) {
  const auto sort =
      std::make_shared<Sort>(table_wrapper, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}}});
  sort->execute();

  Print::print(sort->get_output());

  const auto column_ids = std::vector<ColumnID>({ColumnID{2}, ColumnID{0}});
  const auto aliases = std::vector<std::string>({"col_2", "col_0"});

  const auto alias =
      std::make_shared<Alias>(sort, column_ids, aliases);
  alias->execute();

  const auto& result_table_sorted = alias->get_output();

  Print::print(result_table_sorted);

  for (auto chunk_id = ChunkID{0}; chunk_id < result_table_sorted->chunk_count(); ++chunk_id) {
    const auto& chunk_sorted_by = result_table_sorted->get_chunk(chunk_id)->individually_sorted_by();
    const auto expected_sorted_by =
        std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{1}}};

    ASSERT_EQ(expected_sorted_by, chunk_sorted_by);
  }
}

}  // namespace opossum
