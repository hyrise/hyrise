#include "meta_settings_table.hpp"

#include "hyrise.hpp"
#include "utils/assert.hpp"

namespace opossum {

MetaSettingsTable::MetaSettingsTable()
    : AbstractMetaTable(TableColumnDefinitions{{"name", DataType::String, false},
                                               {"value", DataType::String, false},
                                               {"description", DataType::String, false}}) {}

const std::string& MetaSettingsTable::name() const {
  static const auto name = std::string{"settings"};
  return name;
}

bool MetaSettingsTable::can_update() const { return true; }

std::shared_ptr<Table> MetaSettingsTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
  const auto setting_names = Hyrise::get().settings_manager.setting_names();

  for (const auto& setting_name : setting_names) {
    const auto& setting = Hyrise::get().settings_manager.get_setting(setting_name);
    output_table->append({pmr_string{setting_name}, pmr_string{setting->get()}, pmr_string{setting->description()}});
  }

  return output_table;
}

void MetaSettingsTable::_on_update(const std::vector<AllTypeVariant>& selected_values,
                                   const std::vector<AllTypeVariant>& update_values) {
  const auto& name = std::string{boost::get<pmr_string>(selected_values.at(0))};
  Assert(Hyrise::get().settings_manager.has_setting(name), "No setting named " + name + " found.");

  const auto& value = std::string{boost::get<pmr_string>(update_values.at(1))};
  Hyrise::get().settings_manager.get_setting(name)->set(value);
}

}  // namespace opossum
