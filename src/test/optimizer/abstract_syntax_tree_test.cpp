#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"

namespace opossum {

class AbstractSyntaxTreeTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(AbstractSyntaxTreeTest, ParentTest) {
  const auto table_node = std::make_shared<TableNode>("a");

  ASSERT_EQ(table_node->left(), nullptr);
  ASSERT_EQ(table_node->right(), nullptr);
  ASSERT_EQ(table_node->parent(), nullptr);

  const auto predicate_node = std::make_shared<PredicateNode>("c1", ScanType::OpEquals, "a");
  predicate_node->set_left(table_node);

  ASSERT_EQ(table_node->parent(), predicate_node);
  ASSERT_EQ(predicate_node->left(), table_node);
  ASSERT_EQ(predicate_node->right(), nullptr);
  ASSERT_EQ(predicate_node->parent(), nullptr);

  std::vector<std::string> column_names = {"c1", "c2"};
  const auto projection_node = std::make_shared<ProjectionNode>(column_names);
  projection_node->set_left(predicate_node);

  ASSERT_EQ(predicate_node->parent(), projection_node);
  ASSERT_EQ(projection_node->left(), predicate_node);
  ASSERT_EQ(projection_node->right(), nullptr);
  ASSERT_EQ(projection_node->parent(), nullptr);
}

TEST_F(AbstractSyntaxTreeTest, ChainSameNodesTest) {
  const auto table_node = std::make_shared<TableNode>("a");

  ASSERT_EQ(table_node->left(), nullptr);
  ASSERT_EQ(table_node->right(), nullptr);
  ASSERT_EQ(table_node->parent(), nullptr);

  const auto predicate_node = std::make_shared<PredicateNode>("c1", ScanType::OpEquals, "a");
  predicate_node->set_left(table_node);

  ASSERT_EQ(table_node->parent(), predicate_node);
  ASSERT_EQ(predicate_node->left(), table_node);
  ASSERT_EQ(predicate_node->right(), nullptr);
  ASSERT_EQ(predicate_node->parent(), nullptr);

  const auto predicate_node_2 = std::make_shared<PredicateNode>("c2", ScanType::OpEquals, "b");
  predicate_node_2->set_left(predicate_node);

  ASSERT_EQ(predicate_node->parent(), predicate_node_2);
  ASSERT_EQ(predicate_node_2->left(), predicate_node);
  ASSERT_EQ(predicate_node_2->right(), nullptr);
  ASSERT_EQ(predicate_node_2->parent(), nullptr);

  std::vector<std::string> column_names = {"c1", "c2"};
  const auto projection_node = std::make_shared<ProjectionNode>(column_names);
  projection_node->set_left(predicate_node_2);

  ASSERT_EQ(predicate_node_2->parent(), projection_node);
  ASSERT_EQ(projection_node->left(), predicate_node_2);
  ASSERT_EQ(projection_node->right(), nullptr);
  ASSERT_EQ(projection_node->parent(), nullptr);
}

}  // namespace opossum
