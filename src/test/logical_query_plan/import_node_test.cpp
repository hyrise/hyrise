#include <memory>

#include "base_test.hpp"

#include "expression/expression_utils.hpp"
#include "import_export/file_type.hpp"
#include "logical_query_plan/import_node.hpp"

namespace opossum {

class ImportNodeTest : public BaseTest {
 protected:
  void SetUp() override { _import_node = ImportNode::make("tablename", "filename", FileType::Csv); }

  std::shared_ptr<ImportNode> _import_node;
};

TEST_F(ImportNodeTest, Description) { EXPECT_EQ(_import_node->description(), "[Import] Name: 'tablename'"); }

TEST_F(ImportNodeTest, HashingAndEqualityCheck) {
  const auto another_import_node = ImportNode::make("tablename", "filename", FileType::Csv);
  EXPECT_EQ(*_import_node, *another_import_node);

  EXPECT_EQ(_import_node->hash(), another_import_node->hash());
}

TEST_F(ImportNodeTest, NodeExpressions) { EXPECT_TRUE(_import_node->node_expressions.empty()); }

TEST_F(ImportNodeTest, ColumnExpressions) { EXPECT_TRUE(_import_node->column_expressions().empty()); }

TEST_F(ImportNodeTest, Copy) { EXPECT_EQ(*_import_node, *_import_node->deep_copy()); }

}  // namespace opossum
