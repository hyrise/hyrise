#include "meta_plugins_table.hpp"

#include <boost/algorithm/string.hpp>

#include "hyrise.hpp"

#ifdef __APPLE__
constexpr char DYNAMIC_LIBRARY_SUFFIX[] = ".dylib";
#elif __linux__
constexpr char DYNAMIC_LIBRARY_SUFFIX[] = ".so";
#endif

namespace opossum {

MetaPluginsTable::MetaPluginsTable()
    : AbstractMetaTable(TableColumnDefinitions{{"plugin_name", DataType::String, false}}) {}

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
  const auto file_name = get_full_file_name(std::string{boost::get<pmr_string>(values.at(0))});

  Hyrise::get().plugin_manager.load_plugin(file_name);
}

void MetaPluginsTable::_on_remove(const std::vector<AllTypeVariant>& values) {
  const auto plugin_name = std::string{boost::get<pmr_string>(values.at(0))};
  Hyrise::get().plugin_manager.unload_plugin(plugin_name);
}

std::string MetaPluginsTable::get_full_file_name(const std::string& file_name) const {
  auto extension = std::string{std::filesystem::path{file_name}.extension()};
  boost::algorithm::to_lower(extension);
  if (extension == DYNAMIC_LIBRARY_SUFFIX) {
    return file_name;
  }

  return file_name + DYNAMIC_LIBRARY_SUFFIX;
}

}  // namespace opossum
