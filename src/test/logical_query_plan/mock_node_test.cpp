#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/mock_node.hpp"
#include "optimizer/table_statistics.hpp"

namespace opossum {

class MockNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    auto table = load_table("src/test/tables/int_float_double_string.tbl", Chunk::MAX_SIZE);
    _statistics = std::make_shared<TableStatistics>(table);

    _mock_node = std::make_shared<MockNode>(_statistics);
  }

  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<TableStatistics> _statistics;
};

TEST_F(MockNodeTest, Description) { EXPECT_EQ(_mock_node->description(), "[MockTable]"); }

TEST_F(MockNodeTest, OutputColumnNames) {
  auto& column_names = _mock_node->output_column_names();

  for (ColumnID column_index{0}; column_index < ColumnID{4}; ++column_index) {
    auto expected_name = "MockCol" + std::to_string(column_index);

    EXPECT_EQ(column_names[column_index], expected_name);
    EXPECT_EQ(_mock_node->get_verbose_column_name(column_index), expected_name);
  }
}

TEST_F(MockNodeTest, ColumnNamesWithAlias) {
  auto aliased_node = std::make_shared<MockNode>(_statistics);
  aliased_node->set_alias("foo");

  for (ColumnID column_index{0}; column_index < ColumnID{4}; ++column_index) {
    auto expected_name = "foo.MockCol" + std::to_string(column_index);
    EXPECT_EQ(aliased_node->get_verbose_column_name(column_index), expected_name);
  }
}

TEST_F(MockNodeTest, NoMappingForInputColumns) {
  auto column_ids = _mock_node->output_column_ids_to_input_column_ids();

  EXPECT_EQ(column_ids.size(), _mock_node->output_column_count());

  for (auto column_id : column_ids) {
    EXPECT_EQ(column_id, INVALID_COLUMN_ID);
  }
}

}  // namespace opossum
