#include "base_test.hpp"

#include "../mock_setting.hpp"
#include "operators/table_wrapper.hpp"
#include "utils/meta_tables/meta_settings_table.hpp"

namespace opossum {

class MetaSettingsTest : public BaseTest {
 protected:
  std::shared_ptr<const Table> mock_manipulation_values;
  std::shared_ptr<MockSetting> mock_setting;
  std::shared_ptr<AbstractMetaTable> meta_settings_table;

  void SetUp() {
    Hyrise::reset();
    meta_settings_table = std::make_shared<MetaSettingsTable>();
    const auto column_definitions = meta_settings_table->column_definitions();
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
    table->append({pmr_string{"mock_setting"}, pmr_string{"bar"}, pmr_string{"baz"}});
    auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
    table_wrapper->execute();
    mock_manipulation_values = table_wrapper->get_output();
    mock_setting = std::make_shared<MockSetting>("mock_setting");
    mock_setting->register_at_settings_manager();
  }

  void TearDown() { Hyrise::reset(); }

  const std::shared_ptr<Table> generate_meta_table(const std::shared_ptr<AbstractMetaTable>& table) const {
    return table->_generate();
  }

  void updateTable(const std::shared_ptr<AbstractMetaTable>& table, const std::vector<AllTypeVariant>& selected_values,
                   const std::vector<AllTypeVariant>& update_values) const {
    return table->_update(selected_values, update_values);
  }
};

TEST_F(MetaSettingsTest, IsUpdateable) {
  EXPECT_FALSE(meta_settings_table->can_insert());
  EXPECT_TRUE(meta_settings_table->can_update());
  EXPECT_FALSE(meta_settings_table->can_delete());
}

TEST_F(MetaSettingsTest, TableGeneration) {
  const auto expected_table = std::make_shared<Table>(TableColumnDefinitions{{"name", DataType::String, false},
                                                                             {"value", DataType::String, false},
                                                                             {"description", DataType::String, false}},
                                                      TableType::Data, 5);
  expected_table->append({pmr_string{"mock_setting"}, pmr_string{"mock_value"}, pmr_string{"mock_description"}});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(expected_table));
  table_wrapper->execute();

  const auto meta_table = generate_meta_table(meta_settings_table);
  EXPECT_TABLE_EQ_UNORDERED(meta_table, table_wrapper->get_output());
}

TEST_F(MetaSettingsTest, Update) {
  updateTable(meta_settings_table, mock_manipulation_values->get_row(0), mock_manipulation_values->get_row(0));

  const auto expected_table = std::make_shared<Table>(TableColumnDefinitions{{"name", DataType::String, false},
                                                                             {"value", DataType::String, false},
                                                                             {"description", DataType::String, false}},
                                                      TableType::Data, 5);
  expected_table->append({pmr_string{"mock_setting"}, pmr_string{"bar"}, pmr_string{"mock_description"}});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(expected_table));
  table_wrapper->execute();

  const auto meta_table = generate_meta_table(meta_settings_table);

  EXPECT_EQ(mock_setting->set_calls(), 1);
  EXPECT_TABLE_EQ_UNORDERED(meta_table, table_wrapper->get_output());
}

}  // namespace opossum
