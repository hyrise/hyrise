#include <memory>

#include "base_test.hpp"
#include "utils/assert.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/maintenance/create_view.hpp"
#include "storage/lqp_view.hpp"
#include "storage/table.hpp"

namespace hyrise {

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

  auto create_view = std::make_shared<CreateView>("view_name", view, false);

  EXPECT_EQ(create_view->name(), "CreateView");
}

TEST_F(CreateViewTest, DeepCopy) {
  const auto view_lqp = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});
  const auto view = std::make_shared<LQPView>(view_lqp, std::unordered_map<ColumnID, std::string>{});

  auto create_view = std::make_shared<CreateView>("view_name", view, false);

  create_view->execute();
  EXPECT_EQ(create_view->get_output(), nullptr);

  const auto copy = create_view->deep_copy();
  EXPECT_EQ(copy->executed(), false);
}

TEST_F(CreateViewTest, Execute) {
  const auto view_lqp = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});
  const auto view_in = std::make_shared<LQPView>(view_lqp, std::unordered_map<ColumnID, std::string>{});

  auto create_view = std::make_shared<CreateView>("view_name", view_in, false);
  create_view->execute();

  EXPECT_EQ(create_view->get_output(), nullptr);

  EXPECT_TRUE(Hyrise::get().storage_manager.has_view("view_name"));

  auto view_out = Hyrise::get().storage_manager.get_view("view_name");
  EXPECT_EQ(view_out->lqp->type, LQPNodeType::Mock);

  auto create_view_2 = std::make_shared<CreateView>("view_name", view_in, false);
  EXPECT_ANY_THROW(create_view_2->execute());
}

TEST_F(CreateViewTest, ExecuteWithIfNotExists) {
  const auto view_lqp = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "x"}}});
  const auto view_in = std::make_shared<LQPView>(view_lqp, std::unordered_map<ColumnID, std::string>{});

  auto create_view = std::make_shared<CreateView>("view_name", view_in, true);
  create_view->execute();

  EXPECT_EQ(create_view->get_output(), nullptr);

  EXPECT_TRUE(Hyrise::get().storage_manager.has_view("view_name"));

  auto view_out = Hyrise::get().storage_manager.get_view("view_name");
  EXPECT_EQ(view_out->lqp->type, LQPNodeType::Mock);

  auto create_view_2 = std::make_shared<CreateView>("view_name", view_in, true);
  EXPECT_NO_THROW(create_view_2->execute());
}

}  // namespace hyrise
