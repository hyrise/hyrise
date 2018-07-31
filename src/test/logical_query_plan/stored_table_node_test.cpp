#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class StoredTableNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("t_a", load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE));
    StorageManager::get().add_table("t_b", load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE));

    _stored_table_node = StoredTableNode::make("t_a");
    _a = LQPColumnReference(_stored_table_node, ColumnID{0});
    _b = LQPColumnReference(_stored_table_node, ColumnID{1});

    _stored_table_node->set_excluded_chunk_ids({ChunkID{2}});
  }

  std::shared_ptr<StoredTableNode> _stored_table_node;
  LQPColumnReference _a, _b;
};

TEST_F(StoredTableNodeTest, Description) { EXPECT_EQ(_stored_table_node->description(), "[StoredTable] Name: 't_a'"); }

TEST_F(StoredTableNodeTest, GetColumn) {
  EXPECT_EQ(_stored_table_node->get_column("a"), _a);
  EXPECT_EQ(_stored_table_node->get_column("b"), _b);
}

TEST_F(StoredTableNodeTest, Equals) {
  EXPECT_EQ(*_stored_table_node, *_stored_table_node);

  const auto different_node_a = StoredTableNode::make("t_b");
  different_node_a->set_excluded_chunk_ids({ChunkID{2}});

  const auto different_node_b = StoredTableNode::make("t_a");

  EXPECT_NE(*_stored_table_node, *different_node_a);
  EXPECT_NE(*_stored_table_node, *different_node_b);
}

TEST_F(StoredTableNodeTest, Copy) { EXPECT_EQ(*_stored_table_node->deep_copy(), *_stored_table_node); }

}  // namespace opossum
