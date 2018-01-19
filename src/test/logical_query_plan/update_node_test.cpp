#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/update_node.hpp"

namespace opossum {

class UpdateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    std::vector<std::shared_ptr<LQPExpression>> update_expressions;
    _update_node = std::make_shared<UpdateNode>("table_a", update_expressions);
  }

  std::shared_ptr<UpdateNode> _update_node;
};

TEST_F(UpdateNodeTest, Description) { EXPECT_EQ(_update_node->description(), "[Update] Table: 'table_a'"); }

TEST_F(UpdateNodeTest, TableName) { EXPECT_EQ(_update_node->table_name(), "table_a"); }

}  // namespace opossum
