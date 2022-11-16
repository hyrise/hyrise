#include <memory>

#include "base_test.hpp"
#include "utils/assert.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/maintenance/drop_view.hpp"
#include "storage/lqp_view.hpp"
#include "storage/table.hpp"

namespace hyrise {

class DropViewTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = Hyrise::get().storage_manager;
    auto t1 = std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data);

    sm.add_table("first_table", t1);

    const auto view_lqp = StoredTableNode::make("first_table");
    const auto view = std::make_shared<LQPView>(view_lqp, std::unordered_map<ColumnID, std::string>{});

    sm.add_view("view_name", view);
  }
};

TEST_F(DropViewTest, OperatorName) {
  auto drop_view = std::make_shared<DropView>("view_name", false);

  EXPECT_EQ(drop_view->name(), "DropView");
}

TEST_F(DropViewTest, DeepCopy) {
  auto drop_view = std::make_shared<DropView>("view_name", false);

  drop_view->execute();
  EXPECT_EQ(drop_view->get_output(), nullptr);

  const auto copy = drop_view->deep_copy();
  EXPECT_EQ(copy->executed(), false);
}

TEST_F(DropViewTest, Execute) {
  auto drop_view = std::make_shared<DropView>("view_name", false);
  drop_view->execute();

  EXPECT_EQ(drop_view->get_output(), nullptr);

  EXPECT_FALSE(Hyrise::get().storage_manager.has_view("view_name"));
}

TEST_F(DropViewTest, ExecuteWithIfExists) {
  auto drop_view_1 = std::make_shared<DropView>("view_name", true);
  drop_view_1->execute();

  EXPECT_EQ(drop_view_1->get_output(), nullptr);

  EXPECT_FALSE(Hyrise::get().storage_manager.has_view("view_name"));

  auto drop_view_2 = std::make_shared<DropView>("view_name", true);

  EXPECT_NO_THROW(drop_view_2->execute());

  EXPECT_FALSE(Hyrise::get().storage_manager.has_view("view_name"));
}

}  // namespace hyrise
