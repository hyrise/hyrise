#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/table.hpp"
#include "utils/meta_table_manager.hpp"

namespace opossum {

class StorageManagerTest : public BaseTest {
 protected:
  void SetUp() override {
    auto t1 = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
    auto t2 = std::make_shared<Table>(TableColumnDefinitions{{"b", DataType::Int, false}}, TableType::Data, 4);

    _hyrise_env->storage_manager()->add_table("first_table", t1);
    _hyrise_env->storage_manager()->add_table("second_table", t2);

    const auto v1_lqp = StoredTableNode::make(_hyrise_env, "first_table");
    const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});

    const auto v2_lqp = StoredTableNode::make(_hyrise_env, "second_table");
    const auto v2 = std::make_shared<LQPView>(v2_lqp, std::unordered_map<ColumnID, std::string>{});

    _hyrise_env->storage_manager()->add_view("first_view", std::move(v1));
    _hyrise_env->storage_manager()->add_view("second_view", std::move(v2));

    const auto pp1_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
    const auto pp1 = std::make_shared<PreparedPlan>(pp1_lqp, std::vector<ParameterID>{});

    const auto pp2_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Float, "b"}}, "b");
    const auto pp2 = std::make_shared<PreparedPlan>(pp2_lqp, std::vector<ParameterID>{});

    _hyrise_env->storage_manager()->add_prepared_plan("first_prepared_plan", std::move(pp1));
    _hyrise_env->storage_manager()->add_prepared_plan("second_prepared_plan", std::move(pp2));
  }
};

TEST_F(StorageManagerTest, AddTableTwice) {
  EXPECT_THROW(_hyrise_env->storage_manager()->add_table(
                   "first_table", std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data)),
               std::exception);
  EXPECT_THROW(_hyrise_env->storage_manager()->add_table(
                   "first_view", std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data)),
               std::exception);
}

TEST_F(StorageManagerTest, StatisticCreationOnAddTable) {
  _hyrise_env->storage_manager()->add_table("int_float", load_table("resources/test_data/tbl/int_float.tbl"));

  const auto table = _hyrise_env->storage_manager()->get_table("int_float");
  EXPECT_EQ(table->table_statistics()->row_count, 3.0f);
  const auto chunk = table->get_chunk(ChunkID{0});
  EXPECT_TRUE(chunk->pruning_statistics().has_value());
  EXPECT_EQ(chunk->pruning_statistics()->at(0)->data_type, DataType::Int);
  EXPECT_EQ(chunk->pruning_statistics()->at(1)->data_type, DataType::Float);
}

TEST_F(StorageManagerTest, GetTable) {
  auto t3 = _hyrise_env->storage_manager()->get_table("first_table");
  auto t4 = _hyrise_env->storage_manager()->get_table("second_table");
  EXPECT_THROW(_hyrise_env->storage_manager()->get_table("third_table"), std::exception);
  auto names = std::vector<std::string>{"first_table", "second_table"};
  auto sm_names = _hyrise_env->storage_manager()->table_names();
  std::sort(sm_names.begin(), sm_names.end());
  EXPECT_EQ(sm_names, names);
}

TEST_F(StorageManagerTest, DropTable) {
  _hyrise_env->storage_manager()->drop_table("first_table");
  EXPECT_THROW(_hyrise_env->storage_manager()->get_table("first_table"), std::exception);
  EXPECT_THROW(_hyrise_env->storage_manager()->drop_table("first_table"), std::exception);

  const auto& tables = _hyrise_env->storage_manager()->tables();
  EXPECT_EQ(tables.size(), 1);

  _hyrise_env->storage_manager()->add_table("first_table",
                                            std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data));
  EXPECT_TRUE(_hyrise_env->storage_manager()->has_table("first_table"));
}

TEST_F(StorageManagerTest, DoesNotHaveTable) {
  EXPECT_EQ(_hyrise_env->storage_manager()->has_table("third_table"), false);
}

TEST_F(StorageManagerTest, HasTable) { EXPECT_EQ(_hyrise_env->storage_manager()->has_table("first_table"), true); }

TEST_F(StorageManagerTest, AddViewTwice) {
  const auto v1_lqp = StoredTableNode::make(_hyrise_env, "first_table");
  const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});

  EXPECT_THROW(_hyrise_env->storage_manager()->add_view("first_table", v1), std::exception);
  EXPECT_THROW(_hyrise_env->storage_manager()->add_view("first_view", v1), std::exception);
}

TEST_F(StorageManagerTest, GetView) {
  auto v3 = _hyrise_env->storage_manager()->get_view("first_view");
  auto v4 = _hyrise_env->storage_manager()->get_view("second_view");
  EXPECT_THROW(_hyrise_env->storage_manager()->get_view("third_view"), std::exception);
}

TEST_F(StorageManagerTest, DropView) {
  _hyrise_env->storage_manager()->drop_view("first_view");
  EXPECT_THROW(_hyrise_env->storage_manager()->get_view("first_view"), std::exception);
  EXPECT_THROW(_hyrise_env->storage_manager()->drop_view("first_view"), std::exception);

  const auto& views = _hyrise_env->storage_manager()->views();
  EXPECT_EQ(views.size(), 1);

  const auto v1_lqp = StoredTableNode::make(_hyrise_env, "first_table");
  const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});
  _hyrise_env->storage_manager()->add_view("first_view", v1);
  EXPECT_TRUE(_hyrise_env->storage_manager()->has_view("first_view"));
}

TEST_F(StorageManagerTest, ResetView) {
  Hyrise::reset();
  _hyrise_env_holder = std::make_shared<HyriseEnvironmentHolder>();
  _hyrise_env = _hyrise_env_holder->hyrise_env_ref();
  EXPECT_THROW(_hyrise_env->storage_manager()->get_view("first_view"), std::exception);
}

TEST_F(StorageManagerTest, DoesNotHaveView) {
  EXPECT_EQ(_hyrise_env->storage_manager()->has_view("third_view"), false);
}

TEST_F(StorageManagerTest, HasView) { EXPECT_EQ(_hyrise_env->storage_manager()->has_view("first_view"), true); }

TEST_F(StorageManagerTest, ListViewNames) {
  const auto view_names = _hyrise_env->storage_manager()->view_names();

  EXPECT_EQ(view_names.size(), 2u);

  EXPECT_EQ(view_names[0], "first_view");
  EXPECT_EQ(view_names[1], "second_view");
}

TEST_F(StorageManagerTest, OutputToStream) {
  _hyrise_env->storage_manager()->add_table("third_table", load_table("resources/test_data/tbl/int_int2.tbl", 2));

  std::ostringstream output;
  output << *_hyrise_env->storage_manager();
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

  // first, we remove empty test tables
  _hyrise_env->storage_manager()->drop_table("first_table");
  _hyrise_env->storage_manager()->drop_table("second_table");

  // add a non-empty table
  _hyrise_env->storage_manager()->add_table("third_table", load_table("resources/test_data/tbl/int_float.tbl"));

  _hyrise_env->storage_manager()->export_all_tables_as_csv(opossum::test_data_path);

  const std::string filename = opossum::test_data_path + "/third_table.csv";
  EXPECT_TRUE(std::filesystem::exists(filename));
  std::filesystem::remove(filename);
}

TEST_F(StorageManagerTest, AddPreparedPlanTwice) {
  const auto pp1_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
  const auto pp1 = std::make_shared<PreparedPlan>(pp1_lqp, std::vector<ParameterID>{});

  EXPECT_THROW(_hyrise_env->storage_manager()->add_prepared_plan("first_prepared_plan", pp1), std::exception);
}

TEST_F(StorageManagerTest, GetPreparedPlan) {
  auto pp3 = _hyrise_env->storage_manager()->get_prepared_plan("first_prepared_plan");
  auto pp4 = _hyrise_env->storage_manager()->get_prepared_plan("second_prepared_plan");
  EXPECT_THROW(_hyrise_env->storage_manager()->get_prepared_plan("third_prepared_plan"), std::exception);
}

TEST_F(StorageManagerTest, DropPreparedPlan) {
  _hyrise_env->storage_manager()->drop_prepared_plan("first_prepared_plan");
  EXPECT_THROW(_hyrise_env->storage_manager()->get_prepared_plan("first_prepared_plan"), std::exception);
  EXPECT_THROW(_hyrise_env->storage_manager()->drop_prepared_plan("first_prepared_plan"), std::exception);

  const auto& prepared_plans = _hyrise_env->storage_manager()->prepared_plans();
  EXPECT_EQ(prepared_plans.size(), 1);

  const auto pp_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
  const auto pp = std::make_shared<PreparedPlan>(pp_lqp, std::vector<ParameterID>{});

  _hyrise_env->storage_manager()->add_prepared_plan("first_prepared_plan", pp);
  EXPECT_TRUE(_hyrise_env->storage_manager()->has_prepared_plan("first_prepared_plan"));
}

TEST_F(StorageManagerTest, DoesNotHavePreparedPlan) {
  EXPECT_EQ(_hyrise_env->storage_manager()->has_prepared_plan("third_prepared_plan"), false);
}

TEST_F(StorageManagerTest, HasPreparedPlan) {
  EXPECT_EQ(_hyrise_env->storage_manager()->has_prepared_plan("first_prepared_plan"), true);
}

}  // namespace opossum
