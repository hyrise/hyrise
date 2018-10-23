#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/filesystem.hpp"

namespace opossum {

class StorageManagerTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = StorageManager::get();
    auto t1 = std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data);
    auto t2 = std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data, 4);

    sm.add_table("first_table", t1);
    sm.add_table("second_table", t2);

    const auto v1_lqp = StoredTableNode::make("first_table");
    const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});

    const auto v2_lqp = StoredTableNode::make("second_table");
    const auto v2 = std::make_shared<LQPView>(v2_lqp, std::unordered_map<ColumnID, std::string>{});

    sm.add_lqp_view("first_view", std::move(v1));
    sm.add_lqp_view("second_view", std::move(v2));
  }
};

TEST_F(StorageManagerTest, AddTableTwice) {
  auto& sm = StorageManager::get();
  EXPECT_THROW(sm.add_table("first_table", std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data)),
               std::exception);
  EXPECT_THROW(sm.add_table("first_view", std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data)),
               std::exception);
}

TEST_F(StorageManagerTest, GetTable) {
  auto& sm = StorageManager::get();
  auto t3 = sm.get_table("first_table");
  auto t4 = sm.get_table("second_table");
  EXPECT_THROW(sm.get_table("third_table"), std::exception);
  auto names = std::vector<std::string>{"first_table", "second_table"};
  EXPECT_EQ(sm.table_names(), names);
}

TEST_F(StorageManagerTest, DropTable) {
  auto& sm = StorageManager::get();
  sm.drop_table("first_table");
  EXPECT_THROW(sm.get_table("first_table"), std::exception);
  EXPECT_THROW(sm.drop_table("first_table"), std::exception);
}

TEST_F(StorageManagerTest, DoesNotHaveTable) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_table("third_table"), false);
}

TEST_F(StorageManagerTest, HasTable) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_table("first_table"), true);
}

TEST_F(StorageManagerTest, AddViewTwice) {
  const auto v1_lqp = StoredTableNode::make("first_table");
  const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});

  auto& sm = StorageManager::get();
  EXPECT_THROW(sm.add_lqp_view("first_table", v1), std::exception);
  EXPECT_THROW(sm.add_lqp_view("first_view", v1), std::exception);
}

TEST_F(StorageManagerTest, GetView) {
  auto& sm = StorageManager::get();
  auto v3 = sm.get_view("first_view");
  auto v4 = sm.get_view("second_view");
  EXPECT_THROW(sm.get_view("third_view"), std::exception);
}

TEST_F(StorageManagerTest, DropView) {
  auto& sm = StorageManager::get();
  sm.drop_lqp_view("first_view");
  EXPECT_THROW(sm.get_view("first_view"), std::exception);
  EXPECT_THROW(sm.drop_lqp_view("first_view"), std::exception);
}

TEST_F(StorageManagerTest, ResetView) {
  StorageManager::reset();
  auto& sm = StorageManager::get();
  EXPECT_THROW(sm.get_view("first_view"), std::exception);
}

TEST_F(StorageManagerTest, DoesNotHaveView) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_view("third_view"), false);
}

TEST_F(StorageManagerTest, HasView) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_view("first_view"), true);
}

TEST_F(StorageManagerTest, ListViewNames) {
  auto& sm = StorageManager::get();
  const auto view_names = sm.view_names();

  EXPECT_EQ(view_names.size(), 2u);

  EXPECT_EQ(view_names[0], "first_view");
  EXPECT_EQ(view_names[1], "second_view");
}

TEST_F(StorageManagerTest, Print) {
  auto& sm = StorageManager::get();
  sm.add_table("third_table", load_table("src/test/tables/int_int2.tbl", 2));

  std::ostringstream output;
  sm.print(output);
  auto output_string = output.str();

  EXPECT_TRUE(output_string.find("===== Tables =====") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== table >> first_table << (0 columns, 0 rows in 0 chunks)") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== table >> second_table << (0 columns, 0 rows in 0 chunks)") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== table >> third_table << (2 columns, 4 rows in 2 chunks)") != std::string::npos);

  EXPECT_TRUE(output_string.find("===== Views ======") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== view >> first_view <<") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== view >> second_view <<") != std::string::npos);
}

TEST_F(StorageManagerTest, ExportTables) {
  std::ostringstream output;
  auto& sm = StorageManager::get();

  // first, we remove empty test tables
  sm.drop_table("first_table");
  sm.drop_table("second_table");

  // add a non-empty table
  sm.add_table("third_table", load_table("src/test/tables/int_float.tbl"));

  sm.export_all_tables_as_csv(opossum::test_data_path);

  const std::string filename = opossum::test_data_path + "/third_table.csv";
  EXPECT_TRUE(filesystem::exists(filename));
  filesystem::remove(filename);
}

}  // namespace opossum
