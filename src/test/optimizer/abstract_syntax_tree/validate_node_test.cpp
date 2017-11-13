#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "optimizer/abstract_syntax_tree/validate_node.hpp"

namespace opossum {

class ValidateNodeTest : public BaseTest {
 protected:
  void SetUp() override { _validate_node = std::make_shared<ValidateNode>(); }

  std::shared_ptr<ValidateNode> _validate_node;
};

TEST_F(ValidateNodeTest, Description) { EXPECT_EQ(_validate_node->description(), "[Validate]"); }

}  // namespace opossum
