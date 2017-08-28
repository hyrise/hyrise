#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "optimizer/abstract_syntax_tree/stored_table_node_test.hpp"
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

TEST_F(StoredTableNodeTest, ColumnIdForColumnIdentifier) {
  EXPECT_EQ(_stored_table_node->get_column_id_for_column_identifier({"a", nullopt}), 0);
  EXPECT_EQ(_stored_table_node->get_column_id_for_column_identifier({"a", {"t_a"}}), 0);
  EXPECT_EQ(_stored_table_node->get_column_id_for_column_identifier({"b", {"t_a"}}), 1);
  EXPECT_EQ(_stored_table_node->find_column_id_for_column_identifier({"c", {"t_a"}}), nullopt);
  EXPECT_EQ(_stored_table_node->find_column_id_for_column_identifier({"c", {"garbage"}}), nullopt);
  EXPECT_EQ(_stored_table_node->find_column_id_for_column_identifier({"b", {"garbage"}}), nullopt);
}

}  // namespace opossum
