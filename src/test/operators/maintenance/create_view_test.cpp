#include <memory>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/maintenance/create_view.hpp"
#include "storage/lqp_view.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

class CreateViewTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = StorageManager::get();
    auto t1 = std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data);

    sm.add_table("first_table", t1);
  }
};

TEST_F(CreateViewTest, OperatorName) {
  const auto view_lqp = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});
  const auto view = std::make_shared<LQPView>(view_lqp, std::unordered_map<ColumnID, std::string>{});

  auto cv = std::make_shared<CreateView>("view_name", view);

  EXPECT_EQ(cv->name(), "CreateView");
}

TEST_F(CreateViewTest, DeepCopy) {
  const auto view_lqp = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});
  const auto view = std::make_shared<LQPView>(view_lqp, std::unordered_map<ColumnID, std::string>{});

  auto cv = std::make_shared<CreateView>("view_name", view);

  cv->execute();
  EXPECT_NE(cv->get_output(), nullptr);

  const auto copy = cv->deep_copy();
  EXPECT_EQ(copy->get_output(), nullptr);
}

TEST_F(CreateViewTest, CanCreateViews) {
  const auto view_lqp = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});
  const auto view_in = std::make_shared<LQPView>(view_lqp, std::unordered_map<ColumnID, std::string>{});

  auto cv = std::make_shared<CreateView>("view_name", view_in);
  cv->execute();

  EXPECT_EQ(cv->get_output()->row_count(), 0u) << "CreateView returned non-empty table";

  EXPECT_TRUE(StorageManager::get().has_view("view_name")) << "View was not added";

  auto view_out = StorageManager::get().get_view("view_name");
  EXPECT_EQ(view_out->lqp->type, LQPNodeType::Mock);
}

}  // namespace opossum
