#include <memory>

#include "base_test.hpp"

#include "import_export/file_type.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/exort_node.hpp"

namespace opossum {

class ExportNodeTest : public BaseTest {
 protected:
  void SetUp() override { _export_node = ExportNode::make("tablename", "filename", FileType::Csv); }

  std::shared_ptr<ExportNode> _export_node;
};

TEST_F(ExportNodeTest, Description) { EXPECT_EQ(_Export_node->description(), "[Export] Name: 'tablename'"); }

TEST_F(ExportNodeTest, HashingAndEqualityCheck) {
  const auto another_export_node = ExportNode::make("tablename", "filename", FileType::Csv);
  EXPECT_EQ(*_export_node, *another_export_node);

  EXPECT_EQ(_export_node->hash(), another_export_node->hash());
}

TEST_F(ExportNodeTest, NodeExpressions) { EXPECT_TRUE(_export_node->node_expressions.empty()); }

TEST_F(ExportNodeTest, ColumnExpressions) { EXPECT_TRUE(_export_node->column_expressions().empty()); }

TEST_F(ExportNodeTest, Copy) { EXPECT_EQ(*_export_node, *_export_node->deep_copy()); }

}  // namespace opossum
