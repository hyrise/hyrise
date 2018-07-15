#include "gtest/gtest.h"

#include "operators/alias_operator.hpp"
#include "operators/table_wrapper.hpp"
#include "testing_assert.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class AliasOperatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    const auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int_int.tbl", 1));
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
  EXPECT_EQ(alias_operator->description(DescriptionMode::MultiLine), "Alias [z\nx\ny]");
}

TEST_F(AliasOperatorTest, OutputColumnNames) {
  alias_operator->execute();
  EXPECT_TABLE_EQ_ORDERED(alias_operator->get_output(), load_table("src/test/tables/int_int_int_aliased.tbl"));
}

}  // namespace opossum
