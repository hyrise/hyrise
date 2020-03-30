#include "meta_queries_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaQueriesTable::MetaQueriesTable()
    : AbstractMetaTable(TableColumnDefinitions{{"hash", DataType::String, false},
                                               {"frequency", DataType::Int, false},
                                               {"query_string", DataType::String, false}}) {}

const std::string& MetaQueriesTable::name() const {
  static const auto name = std::string{"cached_queries"};
  return name;
}

std::shared_ptr<Table> MetaQueriesTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
  if (!Hyrise::get().default_pqp_cache) return output_table;

  for (auto iter = Hyrise::get().default_pqp_cache->unsafe_begin();
       iter != Hyrise::get().default_pqp_cache->unsafe_end(); ++iter) {
    const auto& [query_string, _] = *iter;
    std::stringstream query_hex_hash;
    query_hex_hash << std::hex << std::hash<std::string>{}(query_string);

    auto& gdfs_cache = dynamic_cast<GDFSCache<std::string, std::shared_ptr<AbstractOperator>>&>(
        Hyrise::get().default_pqp_cache->unsafe_cache());
    const auto frequency = gdfs_cache.frequency(query_string);

    auto query_single_line{query_string};
    query_single_line.erase(std::remove(query_single_line.begin(), query_single_line.end(), '\n'),
                            query_single_line.end());
    output_table->append(
        {pmr_string{query_hex_hash.str()}, static_cast<int32_t>(frequency), pmr_string{query_single_line}});
  }

  return output_table;
}

}  // namespace opossum
