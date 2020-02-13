#include <memory>

#include "base_test.hpp"

#include "expression/expression_utils.hpp"
#include "import_export/file_type.hpp"
#include "logical_query_plan/export_node.hpp"

namespace opossum {

class ExportNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");

    _export_node = ExportNode::make("table_name", "file_name", FileType::Csv, _mock_node);
  }

  std::shared_ptr<ExportNode> _export_node;
  std::shared_ptr<MockNode> _mock_node;
};

TEST_F(ExportNodeTest, Description) { EXPECT_EQ(_export_node->description(), "[Export] Name: 'table_name'"); }

TEST_F(ExportNodeTest, HashingAndEqualityCheck) {
  const auto another_export_node = ExportNode::make("table_name", "file_name", FileType::Csv, _mock_node);
  EXPECT_EQ(*_export_node, *another_export_node);

  EXPECT_EQ(_export_node->hash(), another_export_node->hash());
}

TEST_F(ExportNodeTest, NodeExpressions) { EXPECT_TRUE(_export_node->node_expressions.empty()); }

TEST_F(ExportNodeTest, ColumnExpressions) { EXPECT_TRUE(_export_node->column_expressions().empty()); }

TEST_F(ExportNodeTest, Copy) { EXPECT_EQ(*_export_node, *_export_node->deep_copy()); }

}  // namespace opossum
