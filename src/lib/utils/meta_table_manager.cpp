#include "meta_table_manager.hpp"

#include "utils/meta_tables/meta_accurate_segments_table.hpp"
#include "utils/meta_tables/meta_chunk_orders_table.hpp"
#include "utils/meta_tables/meta_chunks_table.hpp"
#include "utils/meta_tables/meta_columns_table.hpp"
#include "utils/meta_tables/meta_plugins_table.hpp"
#include "utils/meta_tables/meta_segments_table.hpp"
#include "utils/meta_tables/meta_tables_table.hpp"
namespace opossum {

MetaTableManager::MetaTableManager() {
  const std::list<std::shared_ptr<AbstractMetaTable>> meta_tables = {
      std::make_shared<MetaTablesTable>(),           std::make_shared<MetaChunksTable>(),
      std::make_shared<MetaChunkOrdersTable>(),      std::make_shared<MetaSegmentsTable>(),
      std::make_shared<MetaAccurateSegmentsTable>(), std::make_shared<MetaPluginsTable>(),
  };

  for (const auto& table : meta_tables) {
    _meta_tables[table->name()] = table;
  }

  _table_names.reserve(_meta_tables.size());
  for (const auto& [table_name, _] : _meta_tables) {
    _table_names.emplace_back(table_name);
  }
  std::sort(_table_names.begin(), _table_names.end());
}

bool MetaTableManager::is_meta_table_name(const std::string& name) {
  const auto prefix_len = META_PREFIX.size();
  return name.size() > prefix_len && std::string_view{&name[0], prefix_len} == MetaTableManager::META_PREFIX;
}

const std::vector<std::string>& MetaTableManager::table_names() const { return _table_names; }

bool MetaTableManager::has_table(const std::string& table_name) const { return _meta_tables.count(table_name); }

std::shared_ptr<Table> MetaTableManager::generate_table(const std::string& table_name) const {
  return (_meta_tables.at(table_name))->generate();
}

bool MetaTableManager::can_insert_into(const std::string& table_name) const {
  return (_meta_tables.at(table_name))->can_insert();
}

bool MetaTableManager::can_delete_from(const std::string& table_name) const {
  return (_meta_tables.at(table_name))->can_remove();
}

bool MetaTableManager::can_update(const std::string& table_name) const {
  return (_meta_tables.at(table_name))->can_update();
}

void MetaTableManager::insert_into(const std::string& table_name, const std::shared_ptr<Table>& values) const {
  // TO DO
}

void MetaTableManager::delete_from(const std::string& table_name, const std::shared_ptr<Table>& values) const {
  // TO DO
}

void MetaTableManager::update(const std::string& table_name, const std::shared_ptr<Table>& fields,
                              const std::shared_ptr<Table>& values) const {
  // TO DO
}

}  // namespace opossum
