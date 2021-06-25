#include "base_test.hpp"

#include "logical_query_plan/drop_index_node.hpp"
#include "logical_query_plan/create_index_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "tasks/chunk_compression_task.hpp"
#include "base_test.hpp"
#include "utils/assert.hpp"

#include "operators/maintenance/drop_index.hpp"
#include "operators/maintenance/create_index.hpp"
#include "concurrency/transaction_context.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/table.hpp"
#include "tasks/chunk_compression_task.hpp"

namespace opossum {

class DropIndexNodeTest : public BaseTest {
 public:
  void SetUp() override {
    test_table = load_table("resources/test_data/tbl/string_int_index.tbl", 3);
    Hyrise::get().storage_manager.add_table("t_a", test_table);
    table_node = std::make_shared<StoredTableNode>("t_a");
    column_ids->emplace_back(ColumnID{static_cast<ColumnID>(test_table->column_id_by_name("b"))});

    dummy_table_wrapper = std::make_shared<TableWrapper>(test_table);
    dummy_table_wrapper->execute();

    create_index = std::make_shared<CreateIndex>("some_index", column_ids, true, dummy_table_wrapper);

    auto compression_task_0 = std::make_shared<ChunkCompressionTask>("t_a", ChunkID{0});
    auto compression_task_1 = std::make_shared<ChunkCompressionTask>("t_a", ChunkID{1});

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks({compression_task_0, compression_task_1});

    const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    create_index->set_transaction_context(context);

    create_index->execute();
    context->commit();

    drop_index_node = DropIndexNode::make("some_index", table_node);
  }

  std::shared_ptr<AbstractLQPNode> table_node;
  std::shared_ptr<DropIndexNode> drop_index_node;
  std::shared_ptr<CreateIndex> create_index;
  std::shared_ptr<TableWrapper> dummy_table_wrapper;
  std::shared_ptr<std::vector<ColumnID>> column_ids = std::make_shared<std::vector<ColumnID>>();
  std::shared_ptr<Table> test_table;

};

TEST_F(DropIndexNodeTest, Description) {
  EXPECT_EQ(drop_index_node->description(), "[DropIndex] Name: 'some_index'");
}
//TEST_F(CreateIndexNodeTest, NodeExpressions) { ASSERT_EQ(create_index_node->node_expressions.size(), 0u); }
//
//TEST_F(CreateIndexNodeTest, HashingAndEqualityCheck) {
//  const auto deep_copy_node = create_index_node->deep_copy();
//  EXPECT_EQ(*create_index_node, *deep_copy_node);
//
//  const auto different_create_index_node_a = CreateIndexNode::make("some_index1", false, column_ids, table_node);
//  const auto different_create_index_node_b = CreateIndexNode::make("some_index",  true, column_ids, table_node);
//
//  auto different_column_ids = std::shared_ptr<std::vector<ColumnID>>();
//  different_column_ids->emplace_back(0);
//  different_column_ids->emplace_back(3);
//  const auto different_create_index_node_c = CreateIndexNode::make("some_index", false, different_column_ids, table_node);
//
//  EXPECT_NE(*different_create_index_node_a, *create_index_node);
//  EXPECT_NE(*different_create_index_node_b, *create_index_node);
//  EXPECT_NE(*different_create_index_node_c, *create_index_node);
//
//
//  EXPECT_NE(different_create_index_node_a->hash(), create_index_node->hash());
//  EXPECT_NE(different_create_index_node_b->hash(), create_index_node->hash());
//  EXPECT_NE(different_create_index_node_c->hash(), create_index_node->hash());
//
//}
//
//TEST_F(CreateIndexNodeTest, Copy) { EXPECT_EQ(*create_index_node, *create_index_node->deep_copy()); }

}  // namespace opossum
