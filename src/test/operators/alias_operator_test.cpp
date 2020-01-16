#include "base_test.hpp"

#include "operators/alias_operator.hpp"
#include "operators/table_wrapper.hpp"
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
  EXPECT_EQ(alias_operator->description(DescriptionMode::MultiLine), "Alias [z\nx\ny]");
}

TEST_F(AliasOperatorTest, OutputColumnNames) {
  alias_operator->execute();
  EXPECT_TABLE_EQ_ORDERED(alias_operator->get_output(),
                          load_table("resources/test_data/tbl/int_int_float_aliased.tbl"));
}

}  // namespace opossum
