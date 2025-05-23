#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/table.hpp"
#include "utils/meta_table_manager.hpp"

namespace hyrise {

class StorageManagerTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = Hyrise::get().storage_manager;
    auto t1 = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
    auto t2 =
        std::make_shared<Table>(TableColumnDefinitions{{"b", DataType::Int, false}}, TableType::Data, ChunkOffset{4});

    sm.add_table("first_table", t1);
    sm.add_table("second_table", t2);

    const auto v1_lqp = StoredTableNode::make("first_table");
    const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});

    const auto v2_lqp = StoredTableNode::make("second_table");
    const auto v2 = std::make_shared<LQPView>(v2_lqp, std::unordered_map<ColumnID, std::string>{});

    sm.add_view("first_view", std::move(v1));
    sm.add_view("second_view", std::move(v2));

    const auto pp1_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
    const auto pp1 = std::make_shared<PreparedPlan>(pp1_lqp, std::vector<ParameterID>{});

    const auto pp2_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Float, "b"}}, "b");
    const auto pp2 = std::make_shared<PreparedPlan>(pp2_lqp, std::vector<ParameterID>{});

    sm.add_prepared_plan("first_prepared_plan", std::move(pp1));
    sm.add_prepared_plan("second_prepared_plan", std::move(pp2));
  }
};

TEST_F(StorageManagerTest, AddTableTwice) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_THROW(sm.add_table("first_table", std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data)),
               std::logic_error);
  EXPECT_THROW(sm.add_table("first_view", std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data)),
               std::logic_error);
}

TEST_F(StorageManagerTest, StatisticsCreationOnAddTable) {
  auto& sm = Hyrise::get().storage_manager;
  sm.add_table("int_float", load_table("resources/test_data/tbl/int_float.tbl"));

  const auto table = sm.get_table("int_float");
  EXPECT_EQ(table->table_statistics()->row_count, 3.0);
  const auto chunk = table->get_chunk(ChunkID{0});
  EXPECT_TRUE(chunk->pruning_statistics());
  EXPECT_EQ(chunk->pruning_statistics()->at(0)->data_type, DataType::Int);
  EXPECT_EQ(chunk->pruning_statistics()->at(1)->data_type, DataType::Float);
}

TEST_F(StorageManagerTest, NoSuperfluousStatisticsGeneration) {
  // Do not re-create table/pruning statistics if the table already has them.
  const auto table = load_table("resources/test_data/tbl/int_float.tbl");
  table->set_table_statistics(TableStatistics::from_table(*table));
  generate_chunk_pruning_statistics(table);

  const auto table_statistics = table->table_statistics();
  const auto pruning_statistics = table->get_chunk(ChunkID{0})->pruning_statistics();

  Hyrise::get().storage_manager.add_table("int_float", table);
  EXPECT_EQ(table->table_statistics(), table_statistics);
  EXPECT_EQ(table->get_chunk(ChunkID{0})->pruning_statistics(), pruning_statistics);
}

TEST_F(StorageManagerTest, TableNames) {
  const auto& sm = Hyrise::get().storage_manager;
  const auto names = std::vector<std::string>{"first_table", "second_table"};
  const auto& sm_names = sm.table_names();
  EXPECT_TRUE(std::is_sorted(sm_names.cbegin(), sm_names.cend()));
  EXPECT_EQ(sm_names, names);
}

TEST_F(StorageManagerTest, GetTable) {
  const auto& sm = Hyrise::get().storage_manager;
  const auto t3 = sm.get_table("first_table");
  const auto t4 = sm.get_table("second_table");
  EXPECT_THROW(sm.get_table("third_table"), std::logic_error);
}

TEST_F(StorageManagerTest, DropTable) {
  auto& sm = Hyrise::get().storage_manager;
  sm.drop_table("first_table");
  EXPECT_THROW(sm.get_table("first_table"), std::logic_error);
  EXPECT_THROW(sm.drop_table("first_table"), std::logic_error);

  const auto& tables = sm.tables();
  EXPECT_EQ(tables.size(), 1);

  sm.add_table("first_table", std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data));
  EXPECT_TRUE(sm.has_table("first_table"));
}

TEST_F(StorageManagerTest, DoesNotHaveTable) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_FALSE(sm.has_table("third_table"));
}

TEST_F(StorageManagerTest, HasTable) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_TRUE(sm.has_table("first_table"));
}

TEST_F(StorageManagerTest, AddViewTwice) {
  const auto v1_lqp = StoredTableNode::make("first_table");
  const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});

  auto& sm = Hyrise::get().storage_manager;
  EXPECT_THROW(sm.add_view("first_table", v1), std::logic_error);
  EXPECT_THROW(sm.add_view("first_view", v1), std::logic_error);
}

TEST_F(StorageManagerTest, GetView) {
  auto& sm = Hyrise::get().storage_manager;
  auto v3 = sm.get_view("first_view");
  auto v4 = sm.get_view("second_view");
  EXPECT_THROW(sm.get_view("third_view"), std::logic_error);
}

TEST_F(StorageManagerTest, DropView) {
  auto& sm = Hyrise::get().storage_manager;
  sm.drop_view("first_view");
  EXPECT_THROW(sm.get_view("first_view"), std::logic_error);
  EXPECT_THROW(sm.drop_view("first_view"), std::logic_error);

  const auto& views = sm.views();
  EXPECT_EQ(views.size(), 1);

  const auto v1_lqp = StoredTableNode::make("first_table");
  const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});
  sm.add_view("first_view", v1);
  EXPECT_TRUE(sm.has_view("first_view"));
}

TEST_F(StorageManagerTest, ResetView) {
  Hyrise::reset();
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_THROW(sm.get_view("first_view"), std::logic_error);
}

TEST_F(StorageManagerTest, DoesNotHaveView) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_FALSE(sm.has_view("third_view"));
}

TEST_F(StorageManagerTest, HasView) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_TRUE(sm.has_view("first_view"));
}

TEST_F(StorageManagerTest, ListViewNames) {
  auto& sm = Hyrise::get().storage_manager;
  const auto view_names = sm.view_names();

  EXPECT_EQ(view_names.size(), 2u);

  EXPECT_EQ(view_names[0], "first_view");
  EXPECT_EQ(view_names[1], "second_view");
}

TEST_F(StorageManagerTest, OutputToStream) {
  auto& sm = Hyrise::get().storage_manager;
  sm.add_table("third_table", load_table("resources/test_data/tbl/int_int2.tbl", ChunkOffset{2}));

  std::ostringstream output;
  output << sm;
  auto output_string = output.str();

  EXPECT_TRUE(output_string.find("===== Tables =====") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== table >> first_table << (1 columns, 0 rows in 0 chunks)") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== table >> second_table << (1 columns, 0 rows in 0 chunks)") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== table >> third_table << (2 columns, 4 rows in 2 chunks)") != std::string::npos);

  EXPECT_TRUE(output_string.find("===== Views ======") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== view >> first_view <<") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== view >> second_view <<") != std::string::npos);
}

TEST_F(StorageManagerTest, ExportTables) {
  std::ostringstream output;
  auto& sm = Hyrise::get().storage_manager;

  // first, we remove empty test tables
  sm.drop_table("first_table");
  sm.drop_table("second_table");

  // add a non-empty table
  sm.add_table("third_table", load_table("resources/test_data/tbl/int_float.tbl"));

  sm.export_all_tables_as_csv(test_data_path);

  const std::string filename = test_data_path + "/third_table.csv";
  EXPECT_TRUE(std::filesystem::exists(filename));
  std::filesystem::remove(filename);
}

TEST_F(StorageManagerTest, AddPreparedPlanTwice) {
  auto& sm = Hyrise::get().storage_manager;

  const auto pp1_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
  const auto pp1 = std::make_shared<PreparedPlan>(pp1_lqp, std::vector<ParameterID>{});

  EXPECT_THROW(sm.add_prepared_plan("first_prepared_plan", pp1), std::logic_error);
}

TEST_F(StorageManagerTest, GetPreparedPlan) {
  auto& sm = Hyrise::get().storage_manager;
  auto pp3 = sm.get_prepared_plan("first_prepared_plan");
  auto pp4 = sm.get_prepared_plan("second_prepared_plan");
  EXPECT_THROW(sm.get_prepared_plan("third_prepared_plan"), std::logic_error);
}

TEST_F(StorageManagerTest, DropPreparedPlan) {
  auto& sm = Hyrise::get().storage_manager;
  sm.drop_prepared_plan("first_prepared_plan");
  EXPECT_THROW(sm.get_prepared_plan("first_prepared_plan"), std::logic_error);
  EXPECT_THROW(sm.drop_prepared_plan("first_prepared_plan"), std::logic_error);

  const auto& prepared_plans = sm.prepared_plans();
  EXPECT_EQ(prepared_plans.size(), 1);

  const auto pp_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
  const auto pp = std::make_shared<PreparedPlan>(pp_lqp, std::vector<ParameterID>{});

  sm.add_prepared_plan("first_prepared_plan", pp);
  EXPECT_TRUE(sm.has_prepared_plan("first_prepared_plan"));
}

TEST_F(StorageManagerTest, DoesNotHavePreparedPlan) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_FALSE(sm.has_prepared_plan("third_prepared_plan"));
}

TEST_F(StorageManagerTest, HasPreparedPlan) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_TRUE(sm.has_prepared_plan("first_prepared_plan"));
}

}  // namespace hyrise
