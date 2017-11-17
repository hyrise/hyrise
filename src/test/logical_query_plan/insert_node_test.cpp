#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/insert_node.hpp"

namespace opossum {

class InsertNodeTest : public BaseTest {
 protected:
  void SetUp() override { _insert_node = std::make_shared<InsertNode>("table_a"); }

  std::shared_ptr<InsertNode> _insert_node;
};

TEST_F(InsertNodeTest, Description) { EXPECT_EQ(_insert_node->description(), "[Insert] Into table 'table_a'"); }

TEST_F(InsertNodeTest, TableName) { EXPECT_EQ(_insert_node->table_name(), "table_a"); }

}  // namespace opossum
