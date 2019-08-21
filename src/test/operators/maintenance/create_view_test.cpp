#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "hyrise.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/maintenance/create_view.hpp"
#include "storage/lqp_view.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

class CreateViewTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = Hyrise::get().storage_manager;
    auto t1 = std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data);

    sm.add_table("first_table", t1);
  }
};

TEST_F(CreateViewTest, OperatorName) {
  const auto view_lqp = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});
  const auto view = std::make_shared<LQPView>(view_lqp, std::unordered_map<ColumnID, std::string>{});

  auto cv = std::make_shared<CreateView>("view_name", view, false);

  EXPECT_EQ(cv->name(), "CreateView");
}

TEST_F(CreateViewTest, DeepCopy) {
  const auto view_lqp = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});
  const auto view = std::make_shared<LQPView>(view_lqp, std::unordered_map<ColumnID, std::string>{});

  auto cv = std::make_shared<CreateView>("view_name", view, false);

  cv->execute();
  EXPECT_NE(cv->get_output(), nullptr);

  const auto copy = cv->deep_copy();
  EXPECT_EQ(copy->get_output(), nullptr);
}

TEST_F(CreateViewTest, Execute) {
  const auto view_lqp = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});
  const auto view_in = std::make_shared<LQPView>(view_lqp, std::unordered_map<ColumnID, std::string>{});

  auto cv = std::make_shared<CreateView>("view_name", view_in, false);
  cv->execute();

  EXPECT_EQ(cv->get_output()->row_count(), 0u);

  EXPECT_TRUE(Hyrise::get().storage_manager.has_view("view_name"));

  auto view_out = Hyrise::get().storage_manager.get_view("view_name");
  EXPECT_EQ(view_out->lqp->type, LQPNodeType::Mock);

  auto cv_2 = std::make_shared<CreateView>("view_name", view_in, false);
  EXPECT_ANY_THROW(cv_2->execute());
}

TEST_F(CreateViewTest, ExecuteWithIfNotExists) {
  const auto view_lqp = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});
  const auto view_in = std::make_shared<LQPView>(view_lqp, std::unordered_map<ColumnID, std::string>{});

  auto cv = std::make_shared<CreateView>("view_name", view_in, true);
  cv->execute();

  EXPECT_EQ(cv->get_output()->row_count(), 0u);

  EXPECT_TRUE(Hyrise::get().storage_manager.has_view("view_name"));

  auto view_out = Hyrise::get().storage_manager.get_view("view_name");
  EXPECT_EQ(view_out->lqp->type, LQPNodeType::Mock);

  auto cv_2 = std::make_shared<CreateView>("view_name", view_in, true);
  EXPECT_NO_THROW(cv_2->execute());
}

}  // namespace opossum
