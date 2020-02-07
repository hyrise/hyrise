#include "meta_plugins_table.hpp"

#include "hyrise.hpp"
#include "utils/assert.hpp"

#ifdef __APPLE__
constexpr char DYNAMIC_LIBRARY_SUFFIX[] = ".dylib";
#elif __linux__
constexpr char DYNAMIC_LIBRARY_SUFFIX[] = ".so";
#endif

namespace opossum {

MetaPluginsTable::MetaPluginsTable()
    : _column_definitions(TableColumnDefinitions{{"plugin_name", DataType::String, false}}) {}

const std::string& MetaPluginsTable::name() const {
  static const auto name = std::string{"plugins"};
  return name;
}

bool MetaPluginsTable::can_insert() { return true; }

bool MetaPluginsTable::can_remove() { return true; }

std::shared_ptr<Table> MetaPluginsTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& plugin : Hyrise::get().plugin_manager.loaded_plugins()) {
    output_table->append({pmr_string{plugin}});
  }

  return output_table;
}

void MetaPluginsTable::insert(const std::vector<AllTypeVariant>& values) {
  Assert(values.size() == _column_definitions.size(), "There needs to be one value for every column.");
  Assert(values.at(0).type() == typeid(pmr_string), "Data type must be string.");

  Hyrise::get().plugin_manager.load_plugin(boost::get<pmr_string>(values.at(0)) + DYNAMIC_LIBRARY_SUFFIX);
}

void MetaPluginsTable::remove(const AllTypeVariant& key) {
  Assert(key.type() == typeid(pmr_string), "Data type must be string.");

  Hyrise::get().plugin_manager.unload_plugin(std::string{boost::get<pmr_string>(key)} + DYNAMIC_LIBRARY_SUFFIX);
}

}  // namespace opossum
