#include "meta_settings_table.hpp"

#include "hyrise.hpp"
#include "utils/assert.hpp"

namespace opossum {

MetaSettingsTable::MetaSettingsTable()
    : AbstractMetaTable(TableColumnDefinitions{{"setting_name", DataType::String, false},
                                               {"value", DataType::String, false},
                                               {"description", DataType::String, false}}) {}

const std::string& MetaSettingsTable::name() const {
  static const auto name = std::string{"settings"};
  return name;
}

bool MetaSettingsTable::can_update() const { return true; }

std::shared_ptr<Table> MetaSettingsTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
  const auto settings = Hyrise::get().settings_manager.all_settings();

  for (const auto& setting : settings) {
    output_table->append({pmr_string{setting->name}, pmr_string{setting->get()}, pmr_string{setting->description()}});
  }

  return output_table;
}

void MetaSettingsTable::_on_update(const std::vector<AllTypeVariant>& values) {
  const auto& name = std::string{boost::get<pmr_string>(values.at(0))};
  Assert(Hyrise::get().settings_manager.has_setting(name), "No setting named " + name + " found.");

  const auto& value = std::string{boost::get<pmr_string>(values.at(1))};
  Hyrise::get().settings_manager.get_setting(name)->set(value);
}

}  // namespace opossum
