#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"
#include "optimizer/abstract_syntax_tree/table_scan_node.hpp"

namespace opossum {

class AbstractSyntaxTreeTest : public BaseTest {
 protected:
  void SetUp() override {}

  std::shared_ptr<ExpressionNode> generate_simple_expression(std::string& column_name, ScanType operation, std::string& condition) {
    std::unordered_map<ScanType, ExpressionType> scan_type_to_expression_type = {
      {ScanType::OpBetween, ExpressionType::ExpressionBetween},
      {ScanType::OpEquals, ExpressionType::ExpressionEquals},
      {ScanType::OpNotEquals, ExpressionType::ExpressionNotEquals},
      {ScanType::OpLessThan, ExpressionType::ExpressionLess},
      {ScanType::OpLessThanEquals, ExpressionType::ExpressionLessEq},
      {ScanType::OpGreaterThan, ExpressionType::ExpressionGreater},
      {ScanType::OpGreaterThanEquals, ExpressionType::ExpressionGreaterEq},
      {ScanType::OpLike, ExpressionType::ExpressionLike}
    };

    auto node = std::make_shared<ExpressionNode>(scan_type_to_expression_type[operation]);
  }

};

TEST_F(AbstractSyntaxTreeTest, ParentTest) {
  const auto t_n = std::make_shared<TableNode>("a");

  ASSERT_EQ(t_n->left(), nullptr);
  ASSERT_EQ(t_n->right(), nullptr);
  ASSERT_EQ(t_n->parent().lock(), nullptr);

  const auto ts_n = std::make_shared<TableScanNode>("c1", nullptr, ScanType::OpEquals, "a");
  ts_n->set_left(t_n);

  ASSERT_EQ(t_n->parent().lock(), ts_n);
  ASSERT_EQ(ts_n->left(), t_n);
  ASSERT_EQ(ts_n->right(), nullptr);
  ASSERT_EQ(ts_n->parent().lock(), nullptr);

  std::vector<std::string> column_names = {"c1", "c2"};
  const auto p_n = std::make_shared<ProjectionNode>(column_names);
  p_n->set_left(ts_n);

  ASSERT_EQ(ts_n->parent().lock(), p_n);
  ASSERT_EQ(p_n->left(), ts_n);
  ASSERT_EQ(p_n->right(), nullptr);
  ASSERT_EQ(p_n->parent().lock(), nullptr);
}

TEST_F(AbstractSyntaxTreeTest, ChainSameNodesTest) {
  const auto t_n = std::make_shared<TableNode>("a");

  ASSERT_EQ(t_n->left(), nullptr);
  ASSERT_EQ(t_n->right(), nullptr);
  ASSERT_EQ(t_n->parent().lock(), nullptr);

  const auto ts_n = std::make_shared<TableScanNode>("c1", nullptr, ScanType::OpEquals, "a");
  ts_n->set_left(t_n);

  ASSERT_EQ(t_n->parent().lock(), ts_n);
  ASSERT_EQ(ts_n->left(), t_n);
  ASSERT_EQ(ts_n->right(), nullptr);
  ASSERT_EQ(ts_n->parent().lock(), nullptr);

  const auto ts_n_2 = std::make_shared<TableScanNode>("c2", nullptr, ScanType::OpEquals, "b");
  ts_n_2->set_left(ts_n);

  ASSERT_EQ(ts_n->parent().lock(), ts_n_2);
  ASSERT_EQ(ts_n_2->left(), ts_n);
  ASSERT_EQ(ts_n_2->right(), nullptr);
  ASSERT_EQ(ts_n_2->parent().lock(), nullptr);

  std::vector<std::string> column_names = {"c1", "c2"};
  const auto p_n = std::make_shared<ProjectionNode>(column_names);
  p_n->set_left(ts_n_2);

  ASSERT_EQ(ts_n_2->parent().lock(), p_n);
  ASSERT_EQ(p_n->left(), ts_n_2);
  ASSERT_EQ(p_n->right(), nullptr);
  ASSERT_EQ(p_n->parent().lock(), nullptr);
}

}  // namespace opossum
