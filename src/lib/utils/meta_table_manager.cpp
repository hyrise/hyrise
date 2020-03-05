#include "meta_table_manager.hpp"

#include "utils/meta_tables/meta_chunk_sort_orders_table.hpp"
#include "utils/meta_tables/meta_chunks_table.hpp"
#include "utils/meta_tables/meta_columns_table.hpp"
#include "utils/meta_tables/meta_plugins_table.hpp"
#include "utils/meta_tables/meta_segments_accurate_table.hpp"
#include "utils/meta_tables/meta_segments_table.hpp"
#include "utils/meta_tables/meta_settings_table.hpp"
#include "utils/meta_tables/meta_tables_table.hpp"

namespace opossum {

MetaTableManager::MetaTableManager() {
  const std::vector<std::shared_ptr<AbstractMetaTable>> meta_tables = {
      std::make_shared<MetaTablesTable>(),   std::make_shared<MetaColumnsTable>(),
      std::make_shared<MetaChunksTable>(),   std::make_shared<MetaChunkSortOrdersTable>(),
      std::make_shared<MetaSegmentsTable>(), std::make_shared<MetaSegmentsAccurateTable>(),
      std::make_shared<MetaPluginsTable>(),  std::make_shared<MetaSettingsTable>()};

  _table_names.reserve(_meta_tables.size());
  for (const auto& table : meta_tables) {
    _meta_tables[table->name()] = table;
    _table_names.emplace_back(table->name());
  }
  std::sort(_table_names.begin(), _table_names.end());
}

bool MetaTableManager::is_meta_table_name(const std::string& name) {
  const auto prefix_len = META_PREFIX.size();
  return name.size() > prefix_len && std::string_view{&name[0], prefix_len} == MetaTableManager::META_PREFIX;
}

const std::vector<std::string>& MetaTableManager::table_names() const { return _table_names; }

bool MetaTableManager::has_table(const std::string& table_name) const {
  return _meta_tables.count(_trim_table_name(table_name));
}

std::shared_ptr<Table> MetaTableManager::generate_table(const std::string& table_name) const {
  return (_meta_tables.at(_trim_table_name(table_name)))->_generate();
}

bool MetaTableManager::can_insert_into(const std::string& table_name) const {
  return _meta_tables.at(_trim_table_name(table_name))->can_insert();
}

bool MetaTableManager::can_delete_from(const std::string& table_name) const {
  return _meta_tables.at(_trim_table_name(table_name))->can_delete();
}

bool MetaTableManager::can_update(const std::string& table_name) const {
  return _meta_tables.at(_trim_table_name(table_name))->can_update();
}

void MetaTableManager::insert_into(const std::string& table_name, const std::shared_ptr<const Table>& values) {
  const auto rows = values->get_rows();

  for (const auto& row : rows) {
    _meta_tables.at(table_name)->_insert(row);
  }
}

void MetaTableManager::delete_from(const std::string& table_name, const std::shared_ptr<const Table>& values) {
  const auto& rows = values->get_rows();

  for (const auto& row : rows) {
    _meta_tables.at(table_name)->_remove(row);
  }
}

void MetaTableManager::update(const std::string& table_name, const std::shared_ptr<const Table>& selected_values,
                              const std::shared_ptr<const Table>& update_values) {
  const auto& selected_rows = selected_values->get_rows();
  const auto& update_rows = update_values->get_rows();
  Assert(selected_rows.size() == update_rows.size(), "Selected and updated values need to have the same size.");

  for (size_t row = 0; row < selected_rows.size(); row++) {
    _meta_tables.at(table_name)->_update(selected_rows[row], update_rows[row]);
  }
}

void MetaTableManager::_add(const std::shared_ptr<AbstractMetaTable>& table) {
  _meta_tables[table->name()] = table;
  _table_names.push_back(table->name());
  std::sort(_table_names.begin(), _table_names.end());
}

std::string MetaTableManager::_trim_table_name(const std::string& table_name) {
  return is_meta_table_name(table_name) ? table_name.substr(MetaTableManager::META_PREFIX.size()) : table_name;
}

}  // namespace opossum
