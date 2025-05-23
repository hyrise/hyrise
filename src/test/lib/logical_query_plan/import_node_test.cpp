#include "base_test.hpp"
#include "expression/expression_utils.hpp"
#include "import_export/file_type.hpp"
#include "logical_query_plan/import_node.hpp"
#include "storage/encoding_type.hpp"

namespace hyrise {

class ImportNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _import_node = ImportNode::make("table_name", "file_name", FileType::Csv, EncodingType::Unencoded);
  }

  std::shared_ptr<ImportNode> _import_node;
};

TEST_F(ImportNodeTest, Description) {
  EXPECT_EQ(_import_node->description(), "[Import] Name: 'table_name' (file type Csv, encoding Unencoded)");
}

TEST_F(ImportNodeTest, HashingAndEqualityCheck) {
  const auto another_import_node = ImportNode::make("table_name", "file_name", FileType::Csv, EncodingType::Unencoded);
  EXPECT_EQ(*_import_node, *another_import_node);

  EXPECT_EQ(_import_node->hash(), another_import_node->hash());
}

TEST_F(ImportNodeTest, NodeExpressions) {
  EXPECT_TRUE(_import_node->node_expressions.empty());
}

TEST_F(ImportNodeTest, ColumnExpressions) {
  EXPECT_TRUE(_import_node->output_expressions().empty());
}

TEST_F(ImportNodeTest, Copy) {
  EXPECT_EQ(*_import_node, *_import_node->deep_copy());
}

TEST_F(ImportNodeTest, NoUniqueColumnCombinations) {
  EXPECT_THROW(_import_node->unique_column_combinations(), std::logic_error);
}

TEST_F(ImportNodeTest, NoOrderDependencies) {
  EXPECT_THROW(_import_node->order_dependencies(), std::logic_error);
}

}  // namespace hyrise
