#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class StoredTableNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("t_a", load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE));

    _stored_table_node = std::make_shared<StoredTableNode>("t_a");
    _a = LQPColumnReference(_stored_table_node, ColumnID{0});
    _b = LQPColumnReference(_stored_table_node, ColumnID{1});
  }

  std::shared_ptr<StoredTableNode> _stored_table_node;
  LQPColumnReference _a, _b;
};

TEST_F(StoredTableNodeTest, Description) { EXPECT_EQ(_stored_table_node->description(), "[StoredTable] Name: 't_a'"); }

TEST_F(StoredTableNodeTest, ColumnReferenceByNamedColumnReference) {
  EXPECT_EQ(_stored_table_node->get_column({"a", std::nullopt}), _a);
  EXPECT_EQ(_stored_table_node->get_column({"a", {"t_a"}}), _a);
  EXPECT_EQ(_stored_table_node->get_column({"b", {"t_a"}}), _b);
  EXPECT_EQ(_stored_table_node->find_column({"c", {"t_a"}}), std::nullopt);
  EXPECT_EQ(_stored_table_node->find_column({"c", {"garbage"}}), std::nullopt);
  EXPECT_EQ(_stored_table_node->find_column({"b", {"garbage"}}), std::nullopt);
}

TEST_F(StoredTableNodeTest, ColumnReferenceByOutputColumnID) {
  ASSERT_EQ(_stored_table_node->output_column_references().size(), 2u);
  EXPECT_EQ(_stored_table_node->output_column_references().at(0), _a);
  EXPECT_EQ(_stored_table_node->output_column_references().at(1), _b);
}

TEST_F(StoredTableNodeTest, UnknownTableColumns) {
  EXPECT_EQ(_stored_table_node->find_table_name_origin("invalid_table"), nullptr);
}

TEST_F(StoredTableNodeTest, AliasTable) {
  const auto alias_table_node = _stored_table_node->deep_copy();
  alias_table_node->set_alias({"foo"});

  EXPECT_EQ(alias_table_node->find_table_name_origin("foo"), alias_table_node);
}

TEST_F(StoredTableNodeTest, VerboseColumnNames) {
  EXPECT_EQ(_stored_table_node->get_verbose_column_name(ColumnID{0}), "t_a.a");
  EXPECT_EQ(_stored_table_node->get_verbose_column_name(ColumnID{1}), "t_a.b");
}

TEST_F(StoredTableNodeTest, VerboseColumnNamesWithAlias) {
  const auto node_with_alias = _stored_table_node->deep_copy();
  node_with_alias->set_alias(std::string("foo"));

  EXPECT_EQ(node_with_alias->get_verbose_column_name(ColumnID{0}), "(t_a AS foo).a");
  EXPECT_EQ(node_with_alias->get_verbose_column_name(ColumnID{1}), "(t_a AS foo).b");
}

}  // namespace opossum
