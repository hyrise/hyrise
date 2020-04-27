#include "meta_plugins_table.hpp"

#include <boost/algorithm/string.hpp>

#include "hyrise.hpp"

namespace opossum {

MetaPluginsTable::MetaPluginsTable() : AbstractMetaTable(TableColumnDefinitions{{"name", DataType::String, false}}) {}

const std::string& MetaPluginsTable::name() const {
  static const auto name = std::string{"plugins"};
  return name;
}

bool MetaPluginsTable::can_insert() const { return true; }

bool MetaPluginsTable::can_delete() const { return true; }

std::shared_ptr<Table> MetaPluginsTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& plugin : Hyrise::get().plugin_manager.loaded_plugins()) {
    output_table->append({pmr_string{plugin}});
  }

  return output_table;
}

void MetaPluginsTable::_on_insert(const std::vector<AllTypeVariant>& values) {
  const auto file_name = std::string{boost::get<pmr_string>(values.at(0))};
  Hyrise::get().plugin_manager.load_plugin(file_name);
}

void MetaPluginsTable::_on_remove(const std::vector<AllTypeVariant>& values) {
  const auto plugin_name = std::string{boost::get<pmr_string>(values.at(0))};
  Hyrise::get().plugin_manager.unload_plugin(plugin_name);
}

}  // namespace opossum
