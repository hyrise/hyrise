#include <memory>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/stored_table_node.hpp"
#include "operators/maintenance/drop_view.hpp"
#include "storage/lqp_view.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

class DropViewTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = StorageManager::get();
    auto t1 = std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data);

    sm.add_table("first_table", t1);

    const auto view_lqp = StoredTableNode::make("first_table");
    const auto view = std::make_shared<LQPView>(view_lqp, std::unordered_map<ColumnID, std::string>{});

    sm.add_lqp_view("view_name", view);
  }
};

TEST_F(DropViewTest, OperatorName) {
  auto dv = std::make_shared<DropView>("view_name");

  EXPECT_EQ(dv->name(), "DropView");
}

TEST_F(DropViewTest, DeepCopy) {
  auto dv = std::make_shared<DropView>("view_name");

  dv->execute();
  EXPECT_NE(dv->get_output(), nullptr);

  const auto copy = dv->deep_copy();
  EXPECT_EQ(copy->get_output(), nullptr);
}

TEST_F(DropViewTest, CanDropViews) {
  EXPECT_TRUE(StorageManager::get().has_view("view_name")) << "View not found";

  auto dv = std::make_shared<DropView>("view_name");
  dv->execute();

  EXPECT_EQ(dv->get_output()->row_count(), 0u) << "DropView returned non-empty table";

  EXPECT_FALSE(StorageManager::get().has_view("view_name")) << "View was not removed";
}

}  // namespace opossum
