#include <memory>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/maintenance/create_view.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

class CreateViewTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = StorageManager::get();
    auto t1 = std::make_shared<Table>();

    sm.add_table("first_table", t1);
  }
};

TEST_F(CreateViewTest, OperatorName) {
  auto cv =
      std::make_shared<CreateView>("view_name", MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}}));

  EXPECT_EQ(cv->name(), "CreateView");
}

TEST_F(CreateViewTest, CannotBeRecreated) {
  auto cv =
      std::make_shared<CreateView>("view_name", MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}}));

  EXPECT_ANY_THROW(cv->recreate({}));
}

TEST_F(CreateViewTest, CanCreateViews) {
  auto lqp = StoredTableNode::make("first_table");
  auto cv = std::make_shared<CreateView>("view_name", lqp);
  cv->execute();

  EXPECT_EQ(cv->get_output()->row_count(), 0u) << "CreateView returned non-empty table";

  EXPECT_TRUE(StorageManager::get().has_view("view_name")) << "View was not added";

  auto stored_lqp = StorageManager::get().get_view("view_name");
  EXPECT_EQ(stored_lqp->type(), LQPNodeType::StoredTable);
}

}  // namespace opossum
