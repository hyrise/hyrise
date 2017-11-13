#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class StoredTableNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("t_a", load_table("src/test/tables/int_float.tbl", 0));

    _stored_table_node = std::make_shared<StoredTableNode>("t_a");
  }

  void TearDown() override { StorageManager::get().reset(); }

  std::shared_ptr<StoredTableNode> _stored_table_node;
};

TEST_F(StoredTableNodeTest, Description) { EXPECT_EQ(_stored_table_node->description(), "[StoredTable] Name: 't_a'"); }

TEST_F(StoredTableNodeTest, ColumnsHaveNoInputIDs) {
  for (auto column_id : _stored_table_node->output_column_ids_to_input_column_ids()) {
    EXPECT_EQ(column_id, INVALID_COLUMN_ID);
  }
}

TEST_F(StoredTableNodeTest, ColumnIdForColumnIdentifier) {
  EXPECT_EQ(_stored_table_node->get_column_id_by_named_column_reference({"a", std::nullopt}), 0);
  EXPECT_EQ(_stored_table_node->get_column_id_by_named_column_reference({"a", {"t_a"}}), 0);
  EXPECT_EQ(_stored_table_node->get_column_id_by_named_column_reference({"b", {"t_a"}}), 1);
  EXPECT_EQ(_stored_table_node->find_column_id_by_named_column_reference({"c", {"t_a"}}), std::nullopt);
  EXPECT_EQ(_stored_table_node->find_column_id_by_named_column_reference({"c", {"garbage"}}), std::nullopt);
  EXPECT_EQ(_stored_table_node->find_column_id_by_named_column_reference({"b", {"garbage"}}), std::nullopt);
}

TEST_F(StoredTableNodeTest, OwnOutputColumnIDs) {
  auto column_ids = _stored_table_node->get_output_column_ids_for_table("t_a");

  EXPECT_EQ(column_ids.size(), 2u);
  EXPECT_EQ(column_ids[0], ColumnID{0});
  EXPECT_EQ(column_ids[1], ColumnID{1});
}

TEST_F(StoredTableNodeTest, UnknownTableColumns) {
  auto column_ids = _stored_table_node->get_output_column_ids_for_table("invalid_table");
  EXPECT_EQ(column_ids.size(), 0u);
}

TEST_F(StoredTableNodeTest, AliasTableColumns) {
  const auto alias_table_node = std::make_shared<StoredTableNode>(*_stored_table_node);
  alias_table_node->set_alias({"foo"});

  auto column_ids = alias_table_node->get_output_column_ids_for_table("foo");

  EXPECT_EQ(column_ids.size(), 2u);
  EXPECT_EQ(column_ids[0], ColumnID{0});
  EXPECT_EQ(column_ids[1], ColumnID{1});
}

TEST_F(StoredTableNodeTest, VerboseColumnNames) {
  EXPECT_EQ(_stored_table_node->get_verbose_column_name(ColumnID{0}), "t_a.a");
  EXPECT_EQ(_stored_table_node->get_verbose_column_name(ColumnID{1}), "t_a.b");
}

TEST_F(StoredTableNodeTest, VerboseColumnNamesWithAlias) {
  const auto node_with_alias = std::make_shared<StoredTableNode>(*_stored_table_node);
  node_with_alias->set_alias(std::string("foo"));

  EXPECT_EQ(node_with_alias->get_verbose_column_name(ColumnID{0}), "(t_a AS foo).a");
  EXPECT_EQ(node_with_alias->get_verbose_column_name(ColumnID{1}), "(t_a AS foo).b");
}

}  // namespace opossum
