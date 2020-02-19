#include "meta_settings_table.hpp"

#include "hyrise.hpp"
#include "utils/assert.hpp"

namespace opossum {

MetaSettingsTable::MetaSettingsTable()
    : AbstractMetaTable(),
      _column_definitions(TableColumnDefinitions{{"setting_name", DataType::String, false},
                                                 {"value", DataType::String, false},
                                                 {"description", DataType::String, false}}) {}

const std::string& MetaSettingsTable::name() const {
  static const auto name = std::string{"settings"};
  return name;
}

bool MetaSettingsTable::can_update() { return true; }

std::shared_ptr<Table> MetaSettingsTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& setting : Hyrise::get().settings_manager.all_settings()) {
    output_table->append({pmr_string{setting->name()}, pmr_string{setting->get()}, pmr_string{setting->description()}});
  }

  return output_table;
}

void MetaSettingsTable::_on_insert(const std::vector<AllTypeVariant>& values) {
  const auto& name = std::string{boost::get<pmr_string>(values.at(0))};
  Assert(Hyrise::get().settings_manager.has_setting(name), "No setting " + name + " found.");

  const auto& value = std::string{boost::get<pmr_string>(values.at(1))};
  Hyrise::get().settings_manager.get(name)->set(value);
}

void MetaSettingsTable::_on_remove(const std::vector<AllTypeVariant>& values) {
  // Do nothing here. We only want to let this pass to update.
}

}  // namespace opossum
