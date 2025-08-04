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
    auto t1 = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
    auto t2 =
        std::make_shared<Table>(TableColumnDefinitions{{"b", DataType::Int, false}}, TableType::Data, ChunkOffset{4});

    _add_table(ObjectID{0}, t1);
    _add_table(ObjectID{1}, t2);

    const auto v1_lqp = StoredTableNode::make("first_table");
    const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});

    const auto v2_lqp = StoredTableNode::make("second_table");
    const auto v2 = std::make_shared<LQPView>(v2_lqp, std::unordered_map<ColumnID, std::string>{});

    _add_view(ObjectID{0}, std::move(v1));
    _add_view(ObjectID{1}, std::move(v2));

    const auto pp1_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
    const auto pp1 = std::make_shared<PreparedPlan>(pp1_lqp, std::vector<ParameterID>{});

    const auto pp2_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Float, "b"}}, "b");
    const auto pp2 = std::make_shared<PreparedPlan>(pp2_lqp, std::vector<ParameterID>{});

    _add_prepared_plan(ObjectID{0}, std::move(pp1));
    _add_prepared_plan(ObjectID{1}, std::move(pp2));
  }

  void _add_table(ObjectID table_id, const std::shared_ptr<Table>& table) {
    Hyrise::get().storage_manager._add_table(table_id, table);
  }

  void _drop_table(ObjectID table_id) {
    Hyrise::get().storage_manager._drop_table(table_id);
  }

  void _add_view(ObjectID view_id, const std::shared_ptr<LQPView>& view) {
    Hyrise::get().storage_manager._add_view(view_id, view);
  }

  void _drop_view(ObjectID view_id) {
    Hyrise::get().storage_manager._drop_view(view_id);
  }

  void _add_prepared_plan(ObjectID plan_id, const std::shared_ptr<PreparedPlan>& prepared_plan) {
    Hyrise::get().storage_manager._add_prepared_plan(plan_id, prepared_plan);
  }

  void _drop_prepared_plan(ObjectID plan_id) {
    Hyrise::get().storage_manager._drop_prepared_plan(plan_id);
  }
};

TEST_F(StorageManagerTest, AddObjectTwice) {
  EXPECT_THROW(_add_table(ObjectID{0}, std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data)),
               std::logic_error);
  EXPECT_THROW(_add_view(ObjectID{0}, std::make_shared<LQPView>(nullptr, std::unordered_map<ColumnID, std::string>{})),
               std::logic_error);
  EXPECT_THROW(_add_prepared_plan(ObjectID{0}, std::make_shared<PreparedPlan>(nullptr, std::vector<ParameterID>{})), std::logic_error);
}

TEST_F(StorageManagerTest, StatisticsCreationOnAddTable) {
  _add_table(ObjectID{17}, load_table("resources/test_data/tbl/int_float.tbl"));

  const auto table = Hyrise::get().storage_manager.get_table(ObjectID{17});
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

  _add_table(ObjectID{17}, table);
  EXPECT_EQ(table->table_statistics(), table_statistics);
  EXPECT_EQ(table->get_chunk(ChunkID{0})->pruning_statistics(), pruning_statistics);
}

// TEST_F(StorageManagerTest, TableNames) {
//   const auto& sm = Hyrise::get().storage_manager;
//   const auto names = std::vector<std::string_view>{"first_table", "second_table"};
//   const auto& sm_names = sm.table_names();
//   EXPECT_TRUE(std::is_sorted(sm_names.cbegin(), sm_names.cend()));
//   EXPECT_EQ(sm_names, names);
// }

TEST_F(StorageManagerTest, GetTable) {
  const auto& storage_manager = Hyrise::get().storage_manager;
  const auto t3 = storage_manager.get_table(ObjectID{0});
  const auto t4 = storage_manager.get_table(ObjectID{1});
  EXPECT_THROW(storage_manager.get_table(ObjectID{17}), std::logic_error);
}

TEST_F(StorageManagerTest, DropTable) {
  _drop_table(ObjectID{0});
  EXPECT_THROW(Hyrise::get().storage_manager.get_table(ObjectID{0}), std::logic_error);
  EXPECT_THROW(_drop_table(ObjectID{0}), std::logic_error);

  // const auto& tables = sm.tables();
  // EXPECT_EQ(tables.size(), 1);

  // Adding a table with the same ID is okay. The Catalog should take care that that does not actually happen.
  _add_table(ObjectID{0}, std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("first_table"));
}

TEST_F(StorageManagerTest, HasTable) {
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table(ObjectID{0}));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table(ObjectID{17}));
}

TEST_F(StorageManagerTest, GetView) {
  auto v3 = Hyrise::get().storage_manager.get_view(ObjectID{0});
  auto v4 = Hyrise::get().storage_manager.get_view(ObjectID{1});
  EXPECT_THROW(Hyrise::get().storage_manager.get_view(ObjectID{17}), std::logic_error);
}

TEST_F(StorageManagerTest, DropView) {
  _drop_view(ObjectID{0});
  EXPECT_THROW(Hyrise::get().storage_manager.get_view(ObjectID{0}), std::logic_error);
  EXPECT_THROW(_drop_view(ObjectID{0}), std::logic_error);

  // const auto& views = sm.views();
  // EXPECT_EQ(views.size(), 1);

  // Adding a view with the same ID is okay. The Catalog should take care that that does not actually happen.
  const auto v1_lqp = StoredTableNode::make(ObjectID{0});
  const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});
  _add_view(ObjectID{0}, v1);
  EXPECT_TRUE(Hyrise::get().storage_manager.has_view(ObjectID{0}));
}

TEST_F(StorageManagerTest, ResetView) {
  Hyrise::reset();
  EXPECT_THROW(Hyrise::get().storage_manager.get_view(ObjectID{0}), std::logic_error);
}

TEST_F(StorageManagerTest, HasView) {
  EXPECT_TRUE(Hyrise::get().storage_manager.has_view(ObjectID{0}));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_view(ObjectID{17}));
}

// TEST_F(StorageManagerTest, ListViewNames) {
//   auto& sm = Hyrise::get().storage_manager;
//   const auto view_names = sm.view_names();

//   EXPECT_EQ(view_names.size(), 2u);

//   EXPECT_EQ(view_names[0], "first_view");
//   EXPECT_EQ(view_names[1], "second_view");
// }

// TEST_F(StorageManagerTest, OutputToStream) {
//   auto& sm = Hyrise::get().storage_manager;
//   _add_table("third_table", load_table("resources/test_data/tbl/int_int2.tbl", ChunkOffset{2}));

//   std::ostringstream output;
//   output << sm;
//   auto output_string = output.str();

//   EXPECT_TRUE(output_string.find("===== Tables =====") != std::string::npos);
//   EXPECT_TRUE(output_string.find("==== table >> first_table << (1 columns, 0 rows in 0 chunks)") != std::string::npos);
//   EXPECT_TRUE(output_string.find("==== table >> second_table << (1 columns, 0 rows in 0 chunks)") != std::string::npos);
//   EXPECT_TRUE(output_string.find("==== table >> third_table << (2 columns, 4 rows in 2 chunks)") != std::string::npos);

//   EXPECT_TRUE(output_string.find("===== Views ======") != std::string::npos);
//   EXPECT_TRUE(output_string.find("==== view >> first_view <<") != std::string::npos);
//   EXPECT_TRUE(output_string.find("==== view >> second_view <<") != std::string::npos);
// }

TEST_F(StorageManagerTest, GetPreparedPlan) {
  auto pp3 =   Hyrise::get().storage_manager.get_prepared_plan(ObjectID{0});
  auto pp4 =   Hyrise::get().storage_manager.get_prepared_plan(ObjectID{1});
  EXPECT_THROW(Hyrise::get().storage_manager.get_prepared_plan(ObjectID{17}), std::logic_error);
}

TEST_F(StorageManagerTest, DropPreparedPlan) {
  _drop_prepared_plan(ObjectID{0});
  EXPECT_THROW(Hyrise::get().storage_manager.get_prepared_plan(ObjectID{0}), std::logic_error);
  EXPECT_THROW(_drop_prepared_plan(ObjectID{0}), std::logic_error);

  // const auto& prepared_plans = sm.prepared_plans();
  // EXPECT_EQ(prepared_plans.size(), 1);

  // Adding a prepared plan with the same ID is okay. The Catalog should take care that that does not actually happen.
  const auto pp_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
  const auto pp = std::make_shared<PreparedPlan>(pp_lqp, std::vector<ParameterID>{});
  _add_prepared_plan(ObjectID{0}, pp);
  EXPECT_TRUE(Hyrise::get().storage_manager.has_prepared_plan(ObjectID{0}));
}

TEST_F(StorageManagerTest, HasPreparedPlan) {
  EXPECT_TRUE( Hyrise::get().storage_manager.has_prepared_plan(ObjectID{0}));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_prepared_plan(ObjectID{17}));
}

}  // namespace hyrise
