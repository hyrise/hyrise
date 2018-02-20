#include "gtest/gtest.h"

#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace opossum {

class DropViewNodeTest : public ::testing::Test {
 public:
  void SetUp() override { _drop_view_node = DropViewNode::make("some_view"); }

  std::shared_ptr<DropViewNode> _drop_view_node;
};

TEST_F(DropViewNodeTest, ShallowEquals) {
  EXPECT_TRUE(_drop_view_node->shallow_equals(*_drop_view_node));

  const auto other_drop_view_node = DropViewNode::make("some_other_view");
  EXPECT_FALSE(_drop_view_node->shallow_equals(*other_drop_view_node));
}

}  // namespace opossum
