#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/prepared_plan.hpp"
#include "storage/table.hpp"
#include "utils/meta_table_manager.hpp"

namespace hyrise {

class CatalogTest : public BaseTest {
 protected:
  void SetUp() override {
    _table = Table::create_dummy_table({{"a", DataType::Int, false}});
    const auto lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "b"}}, "b");
    _view = std::make_shared<LQPView>(lqp);
    _prepared_plan = std::make_shared<PreparedPlan>(lqp, std::vector<ParameterID>{});
  }

  std::shared_ptr<Table> _table;
  std::shared_ptr<LQPView> _view;
  std::shared_ptr<PreparedPlan> _prepared_plan;
};

TEST_F(CatalogTest, ResolveStoredObject) {
  Hyrise::get().catalog.add_table("table", _table);
  Hyrise::get().catalog.add_view("view", _view);
  Hyrise::get().catalog.add_prepared_plan("table", _prepared_plan);

  EXPECT_EQ(Hyrise::get().catalog.resolve_stored_object("table"), std::make_pair(ObjectType::Table, ObjectID{0}));
  EXPECT_EQ(Hyrise::get().catalog.resolve_stored_object("view"), std::make_pair(ObjectType::View, ObjectID{0}));

  EXPECT_EQ(Hyrise::get().catalog.resolve_stored_object("unknown_table"), INVALID_OBJECT_INFO);
  EXPECT_EQ(Hyrise::get().catalog.resolve_stored_object("unknown_view"), INVALID_OBJECT_INFO);

  // Prepared plans are not resolved because they cannot be queried in a SELECT statement.
  EXPECT_EQ(Hyrise::get().catalog.resolve_stored_object("prepared_plan"), INVALID_OBJECT_INFO);
}

TEST_F(CatalogTest, ManageTables) {
  // Initially, table "a" is unknown.
  EXPECT_FALSE(Hyrise::get().catalog.has_table("a"));
  EXPECT_EQ(Hyrise::get().catalog.table_id("a"), INVALID_OBJECT_ID);
  EXPECT_THROW(Hyrise::get().catalog.table_name(ObjectID{0}), std::logic_error);
  EXPECT_THROW(Hyrise::get().catalog.drop_table(ObjectID{0}), std::logic_error);
  EXPECT_THROW(Hyrise::get().catalog.drop_table("a"), std::logic_error);

  // After adding the table, metadata should be known.
  const auto table_id_a = Hyrise::get().catalog.add_table("a", _table);
  EXPECT_TRUE(Hyrise::get().catalog.has_table("a"));
  EXPECT_EQ(Hyrise::get().catalog.table_id("a"), table_id_a);
  EXPECT_EQ(Hyrise::get().catalog.table_name(table_id_a), "a");

  // Catalog is responsible for adding the table to StorageManager.
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table(table_id_a));
  EXPECT_EQ(Hyrise::get().storage_manager.get_table(table_id_a), _table);

  // Table with same name, or same name as a view, is not allowed.
  EXPECT_THROW(Hyrise::get().catalog.add_table("a", _table), std::logic_error);
  Hyrise::get().catalog.add_view("view", _view);
  EXPECT_THROW(Hyrise::get().catalog.add_table("view", _table), std::logic_error);

  // Adding another table should result in a new ID. The table can have the same name as a prepared plan.
  Hyrise::get().catalog.add_prepared_plan("b", _prepared_plan);
  const auto table_id_b = Hyrise::get().catalog.add_table("b", _table);
  EXPECT_TRUE(Hyrise::get().catalog.has_table("b"));
  EXPECT_EQ(Hyrise::get().catalog.table_id("b"), table_id_b);
  EXPECT_EQ(Hyrise::get().catalog.table_name(table_id_b), "b");
  EXPECT_NE(table_id_b, table_id_a);

  // Drop tables. Tables should also be removed from StorageManager.
  Hyrise::get().catalog.drop_table("a");
  Hyrise::get().catalog.drop_table(table_id_b);
  EXPECT_FALSE(Hyrise::get().catalog.has_table("a"));
  EXPECT_FALSE(Hyrise::get().catalog.has_table("b"));
  EXPECT_EQ(Hyrise::get().catalog.table_id("a"), INVALID_OBJECT_ID);
  EXPECT_EQ(Hyrise::get().catalog.table_id("b"), INVALID_OBJECT_ID);
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table(table_id_a));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table(table_id_b));
  EXPECT_THROW(Hyrise::get().storage_manager.get_table(table_id_a), std::logic_error);
  EXPECT_THROW(Hyrise::get().storage_manager.get_table(table_id_b), std::logic_error);

  // Now, we can add a table named "a" again. Its ID should be unique, though.
  const auto new_table_id_a = Hyrise::get().catalog.add_table("a", _table);
  EXPECT_TRUE(Hyrise::get().catalog.has_table("a"));
  EXPECT_EQ(Hyrise::get().catalog.table_id("a"), new_table_id_a);
  EXPECT_EQ(Hyrise::get().catalog.table_name(new_table_id_a), "a");
  EXPECT_NE(new_table_id_a, table_id_a);
  EXPECT_NE(new_table_id_a, table_id_b);
}

TEST_F(CatalogTest, ManageViews) {
  // Initially, view "a" is unknown.
  EXPECT_FALSE(Hyrise::get().catalog.has_view("a"));
  EXPECT_EQ(Hyrise::get().catalog.view_id("a"), INVALID_OBJECT_ID);
  EXPECT_THROW(Hyrise::get().catalog.view_name(ObjectID{0}), std::logic_error);
  EXPECT_THROW(Hyrise::get().catalog.drop_view(ObjectID{0}), std::logic_error);
  EXPECT_THROW(Hyrise::get().catalog.drop_view("a"), std::logic_error);

  // After adding the view, metadata should be known.
  const auto view_id_a = Hyrise::get().catalog.add_view("a", _view);
  EXPECT_TRUE(Hyrise::get().catalog.has_view("a"));
  EXPECT_EQ(Hyrise::get().catalog.view_id("a"), view_id_a);
  EXPECT_EQ(Hyrise::get().catalog.view_name(view_id_a), "a");

  // Catalog is responsible for adding the view to StorageManager.
  EXPECT_TRUE(Hyrise::get().storage_manager.has_view(view_id_a));
  // The returned view should be a copy (because the LQP might be changed by the optimizer).
  EXPECT_NE(Hyrise::get().storage_manager.get_view(view_id_a)->lqp, _view->lqp);
  EXPECT_LQP_EQ(Hyrise::get().storage_manager.get_view(view_id_a)->lqp, _view->lqp);

  // View with same name, or same name as a table, is not allowed.
  EXPECT_THROW(Hyrise::get().catalog.add_view("a", _view), std::logic_error);
  Hyrise::get().catalog.add_table("table", _table);
  EXPECT_THROW(Hyrise::get().catalog.add_view("table", _view), std::logic_error);

  // Adding another view should result in a new ID. The view can have the same name as a prepared plan.
  Hyrise::get().catalog.add_prepared_plan("b", _prepared_plan);
  const auto view_id_b = Hyrise::get().catalog.add_view("b", _view);
  EXPECT_TRUE(Hyrise::get().catalog.has_view("b"));
  EXPECT_EQ(Hyrise::get().catalog.view_id("b"), view_id_b);
  EXPECT_EQ(Hyrise::get().catalog.view_name(view_id_b), "b");
  EXPECT_NE(view_id_b, view_id_a);

  // Drop views. Tables should also be removed from StorageManager.
  Hyrise::get().catalog.drop_view("a");
  Hyrise::get().catalog.drop_view(view_id_b);
  EXPECT_FALSE(Hyrise::get().catalog.has_view("a"));
  EXPECT_FALSE(Hyrise::get().catalog.has_view("b"));
  EXPECT_EQ(Hyrise::get().catalog.view_id("a"), INVALID_OBJECT_ID);
  EXPECT_EQ(Hyrise::get().catalog.view_id("b"), INVALID_OBJECT_ID);
  EXPECT_FALSE(Hyrise::get().storage_manager.has_view(view_id_a));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_view(view_id_b));
  EXPECT_THROW(Hyrise::get().storage_manager.get_view(view_id_a), std::logic_error);
  EXPECT_THROW(Hyrise::get().storage_manager.get_view(view_id_b), std::logic_error);

  // Now, we can add a view named "a" again. Its ID should be unique, though.
  const auto new_view_id_a = Hyrise::get().catalog.add_view("a", _view);
  EXPECT_TRUE(Hyrise::get().catalog.has_view("a"));
  EXPECT_EQ(Hyrise::get().catalog.view_id("a"), new_view_id_a);
  EXPECT_EQ(Hyrise::get().catalog.view_name(new_view_id_a), "a");
  EXPECT_NE(new_view_id_a, view_id_a);
  EXPECT_NE(new_view_id_a, view_id_b);
}

TEST_F(CatalogTest, ManagePreparedPlans) {
  // Initially, prepared plan "a" is unknown.
  EXPECT_FALSE(Hyrise::get().catalog.has_prepared_plan("a"));
  EXPECT_EQ(Hyrise::get().catalog.prepared_plan_id("a"), INVALID_OBJECT_ID);
  EXPECT_THROW(Hyrise::get().catalog.prepared_plan_name(ObjectID{0}), std::logic_error);
  EXPECT_THROW(Hyrise::get().catalog.drop_prepared_plan(ObjectID{0}), std::logic_error);
  EXPECT_THROW(Hyrise::get().catalog.drop_prepared_plan("a"), std::logic_error);

  // After adding the prepared plan, metadata should be known.
  const auto plan_id_a = Hyrise::get().catalog.add_prepared_plan("a", _prepared_plan);
  EXPECT_TRUE(Hyrise::get().catalog.has_prepared_plan("a"));
  EXPECT_EQ(Hyrise::get().catalog.prepared_plan_id("a"), plan_id_a);
  EXPECT_EQ(Hyrise::get().catalog.prepared_plan_name(plan_id_a), "a");

  // Catalog is responsible for adding the prepared plan to StorageManager.
  EXPECT_TRUE(Hyrise::get().storage_manager.has_prepared_plan(plan_id_a));
  EXPECT_EQ(Hyrise::get().storage_manager.get_prepared_plan(plan_id_a), _prepared_plan);

  // Prepared plan with same name, is not allowed.
  EXPECT_THROW(Hyrise::get().catalog.add_prepared_plan("a", _prepared_plan), std::logic_error);

  // Adding another prepared plan should result in a new ID. The prepared plan can have the same name as a view or a
  // table.
  Hyrise::get().catalog.add_table("b", _table);
  Hyrise::get().catalog.add_view("c", _view);
  const auto plan_id_b = Hyrise::get().catalog.add_prepared_plan("b", _prepared_plan);
  const auto plan_id_c = Hyrise::get().catalog.add_prepared_plan("c", _prepared_plan);
  EXPECT_TRUE(Hyrise::get().catalog.has_prepared_plan("b"));
  EXPECT_TRUE(Hyrise::get().catalog.has_prepared_plan("c"));
  EXPECT_EQ(Hyrise::get().catalog.prepared_plan_id("b"), plan_id_b);
  EXPECT_EQ(Hyrise::get().catalog.prepared_plan_id("c"), plan_id_c);
  EXPECT_EQ(Hyrise::get().catalog.prepared_plan_name(plan_id_b), "b");
  EXPECT_EQ(Hyrise::get().catalog.prepared_plan_name(plan_id_c), "c");
  EXPECT_NE(plan_id_b, plan_id_a);
  EXPECT_NE(plan_id_c, plan_id_a);
  EXPECT_NE(plan_id_c, plan_id_b);

  // Drop prepared plans. Prepared plans should also be removed from StorageManager.
  Hyrise::get().catalog.drop_prepared_plan("a");
  Hyrise::get().catalog.drop_prepared_plan(plan_id_b);
  Hyrise::get().catalog.drop_prepared_plan(plan_id_c);
  EXPECT_FALSE(Hyrise::get().catalog.has_prepared_plan("a"));
  EXPECT_FALSE(Hyrise::get().catalog.has_prepared_plan("b"));
  EXPECT_FALSE(Hyrise::get().catalog.has_prepared_plan("c"));
  EXPECT_EQ(Hyrise::get().catalog.prepared_plan_id("a"), INVALID_OBJECT_ID);
  EXPECT_EQ(Hyrise::get().catalog.prepared_plan_id("b"), INVALID_OBJECT_ID);
  EXPECT_EQ(Hyrise::get().catalog.prepared_plan_id("c"), INVALID_OBJECT_ID);
  EXPECT_FALSE(Hyrise::get().storage_manager.has_prepared_plan(plan_id_a));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_prepared_plan(plan_id_b));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_prepared_plan(plan_id_c));
  EXPECT_THROW(Hyrise::get().storage_manager.get_prepared_plan(plan_id_a), std::logic_error);
  EXPECT_THROW(Hyrise::get().storage_manager.get_prepared_plan(plan_id_b), std::logic_error);
  EXPECT_THROW(Hyrise::get().storage_manager.get_prepared_plan(plan_id_c), std::logic_error);

  // Now, we can add a prepared_plan named "a" again. Its ID should be unique, though.
  const auto new_plan_id_a = Hyrise::get().catalog.add_prepared_plan("a", _prepared_plan);
  EXPECT_TRUE(Hyrise::get().catalog.has_prepared_plan("a"));
  EXPECT_EQ(Hyrise::get().catalog.prepared_plan_id("a"), new_plan_id_a);
  EXPECT_EQ(Hyrise::get().catalog.prepared_plan_name(new_plan_id_a), "a");
  EXPECT_NE(new_plan_id_a, plan_id_a);
  EXPECT_NE(new_plan_id_a, plan_id_b);
  EXPECT_NE(new_plan_id_a, plan_id_c);
}

TEST_F(CatalogTest, TableNamesAndIDs) {
  Hyrise::get().catalog.add_table("table_a", _table);
  Hyrise::get().catalog.add_table("table_b", _table);
  Hyrise::get().catalog.add_table("table_c", _table);
  Hyrise::get().catalog.drop_table("table_b");

  const auto names = std::vector<std::string>{"table_a", "table_c"};
  const auto catalog_names = Hyrise::get().catalog.table_names();
  EXPECT_TRUE(std::ranges::is_sorted(catalog_names));
  EXPECT_EQ(catalog_names, names);

  const auto catalog_table_ids = Hyrise::get().catalog.table_ids();
  EXPECT_EQ(catalog_table_ids.size(), 2);
  EXPECT_TRUE(catalog_table_ids.contains("table_a"));
  EXPECT_EQ(catalog_table_ids.at("table_a"), ObjectID{0});
  EXPECT_TRUE(catalog_table_ids.contains("table_c"));
  EXPECT_EQ(catalog_table_ids.at("table_c"), ObjectID{2});

  const auto catalog_tables = Hyrise::get().catalog.tables();
  EXPECT_EQ(catalog_tables.size(), 2);
  EXPECT_TRUE(catalog_tables.contains("table_a"));
  EXPECT_EQ(catalog_tables.at("table_a"), _table);
  EXPECT_TRUE(catalog_tables.contains("table_c"));
  EXPECT_EQ(catalog_tables.at("table_c"), _table);
}

}  // namespace hyrise
