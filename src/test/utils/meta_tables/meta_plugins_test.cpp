#include "base_test.hpp"

#include "operators/table_wrapper.hpp"
#include "utils/meta_tables/meta_plugins_table.hpp"

#include "../plugin_test_utils.hpp"

namespace opossum {

class MetaPluginsTest : public BaseTest {
 protected:
  std::shared_ptr<const Table> mock_manipulation_values;
  std::shared_ptr<AbstractSetting> mock_setting;
  std::shared_ptr<AbstractMetaTable> meta_plugins_table;

  void SetUp() {
    Hyrise::reset();
    meta_plugins_table = std::make_shared<MetaPluginsTable>();
    const auto column_definitions = meta_plugins_table->column_definitions();
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
    table->append({pmr_string{build_dylib_path("libhyriseTestPlugin")}});
    auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
    table_wrapper->execute();
    mock_manipulation_values = table_wrapper->get_output();
  }

  void TearDown() { Hyrise::reset(); }

  const std::shared_ptr<Table> generate_meta_table(const std::shared_ptr<AbstractMetaTable>& table) const {
    return table->_generate();
  }

  void delete_from(const std::shared_ptr<AbstractMetaTable>& table, const std::vector<AllTypeVariant>& values) {
    return table->_remove(values);
  }

  void insert_into(const std::shared_ptr<AbstractMetaTable>& table, const std::vector<AllTypeVariant>& values) {
    return table->_insert(values);
  }
};

TEST_F(MetaPluginsTest, IsMutable) {
  EXPECT_TRUE(meta_plugins_table->can_insert());
  EXPECT_FALSE(meta_plugins_table->can_update());
  EXPECT_TRUE(meta_plugins_table->can_delete());
}

TEST_F(MetaPluginsTest, TableGeneration) {
  Hyrise::get().plugin_manager.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  const auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"name", DataType::String, false}}, TableType::Data, 5);
  expected_table->append({pmr_string{"hyriseTestPlugin"}});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(expected_table));
  table_wrapper->execute();

  const auto meta_table = generate_meta_table(meta_plugins_table);
  EXPECT_TABLE_EQ_UNORDERED(meta_table, table_wrapper->get_output());
}

TEST_F(MetaPluginsTest, Insert) {
  const auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"name", DataType::String, false}}, TableType::Data, 5);
  expected_table->append({pmr_string{"hyriseTestPlugin"}});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(expected_table));
  table_wrapper->execute();

  EXPECT_EQ(Hyrise::get().plugin_manager.loaded_plugins().size(), 0);

  insert_into(meta_plugins_table, mock_manipulation_values->get_row(0));

  const auto meta_table = generate_meta_table(meta_plugins_table);
  EXPECT_TABLE_EQ_UNORDERED(meta_table, table_wrapper->get_output());
  EXPECT_EQ(Hyrise::get().plugin_manager.loaded_plugins().at(0), "hyriseTestPlugin");
}

TEST_F(MetaPluginsTest, Delete) {
  Hyrise::get().plugin_manager.load_plugin(build_dylib_path("libhyriseTestPlugin"));

  auto values = std::vector<AllTypeVariant>();
  values.push_back(AllTypeVariant{pmr_string{"hyriseTestPlugin"}});

  delete_from(meta_plugins_table, values);

  EXPECT_EQ(Hyrise::get().plugin_manager.loaded_plugins().size(), 0);
}

}  // namespace opossum
