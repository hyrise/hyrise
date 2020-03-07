#include "../base_test.hpp"

#include "./meta_tables/meta_mock_table.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "utils/load_table.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/meta_tables/meta_chunk_sort_orders_table.hpp"
#include "utils/meta_tables/meta_chunks_table.hpp"
#include "utils/meta_tables/meta_columns_table.hpp"
#include "utils/meta_tables/meta_plugins_table.hpp"
#include "utils/meta_tables/meta_segments_accurate_table.hpp"
#include "utils/meta_tables/meta_segments_table.hpp"
#include "utils/meta_tables/meta_settings_table.hpp"
#include "utils/meta_tables/meta_tables_table.hpp"

namespace opossum {

using MetaTable = std::shared_ptr<AbstractMetaTable>;
using MetaTables = std::vector<MetaTable>;
using MetaTableNames = std::vector<std::string>;

class MetaTableManagerTest : public BaseTest {
 public:
  static MetaTables meta_tables() {
    return {std::make_shared<MetaTablesTable>(),   std::make_shared<MetaColumnsTable>(),
            std::make_shared<MetaChunksTable>(),   std::make_shared<MetaChunkSortOrdersTable>(),
            std::make_shared<MetaSegmentsTable>(), std::make_shared<MetaSegmentsAccurateTable>(),
            std::make_shared<MetaPluginsTable>(),  std::make_shared<MetaSettingsTable>()};
  }

  static MetaTableNames meta_table_names() {
    MetaTableNames names;
    for (auto& table : MetaTableManagerTest::meta_tables()) {
      names.push_back(table->name());
    }

    return names;
  }

  // We need this as the add method of MetaTableManager is protected.
  // Won't compile if add is not called by test class, which is a friend of MetaTableManager.
  static void add_meta_table(const MetaTable& table) { Hyrise::get().meta_table_manager._add(table); }

 protected:
  std::shared_ptr<const Table> mock_manipulation_values;

  void SetUp() {
    Hyrise::reset();

    const auto column_definitions = MetaMockTable().column_definitions();
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
    table->append({pmr_string{"foo"}});
    auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
    table_wrapper->execute();
    mock_manipulation_values = table_wrapper->get_output();
  }

  void TearDown() { Hyrise::reset(); }
};

class MetaTableManagerMultiTablesTest : public MetaTableManagerTest, public ::testing::WithParamInterface<MetaTable> {};

auto meta_table_manager_test_formatter = [](const ::testing::TestParamInfo<MetaTable> info) {
  auto stream = std::stringstream{};
  stream << info.param->name();

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(MetaTableManager, MetaTableManagerMultiTablesTest,
                         ::testing::ValuesIn(MetaTableManagerTest::meta_tables()), meta_table_manager_test_formatter);

TEST_F(MetaTableManagerTest, ListAllTables) {
  auto table_names = MetaTableManagerTest::meta_table_names();
  std::sort(table_names.begin(), table_names.end());

  EXPECT_EQ(Hyrise::get().meta_table_manager.table_names(), table_names);
}

TEST_F(MetaTableManagerTest, ForwardsMethodCalls) {
  const auto mock_table = std::make_shared<MetaMockTable>();
  auto& mtm = Hyrise::get().meta_table_manager;

  MetaTableManagerTest::add_meta_table(mock_table);
  mtm.insert_into(mock_table->name(), mock_manipulation_values);
  mtm.delete_from(mock_table->name(), mock_manipulation_values);
  mtm.update(mock_table->name(), mock_manipulation_values, mock_manipulation_values);

  EXPECT_EQ(mock_table->insert_calls(), 1);
  EXPECT_EQ(mock_table->remove_calls(), 1);
  EXPECT_EQ(mock_table->update_calls(), 1);
}

TEST_P(MetaTableManagerMultiTablesTest, HasAllTables) {
  EXPECT_TRUE(Hyrise::get().meta_table_manager.has_table(GetParam()->name()));
}

TEST_P(MetaTableManagerMultiTablesTest, ForwardsMutationInfo) {
  const auto& table = GetParam();
  const auto& mtm = Hyrise::get().meta_table_manager;
  EXPECT_EQ(mtm.can_insert_into(table->name()), table->can_insert());
  EXPECT_EQ(mtm.can_delete_from(table->name()), table->can_delete());
  EXPECT_EQ(mtm.can_update(table->name()), table->can_update());
}
}  // namespace opossum
