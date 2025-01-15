#include "table_column_definition.hpp"

#include <cstddef>
#include <string>

#include <boost/container_hash/hash.hpp>

#include "all_type_variant.hpp"

namespace hyrise {

TableColumnDefinition::TableColumnDefinition(const std::string& init_name, const DataType init_data_type,
                                             const bool init_nullable)
  : TableColumnDefinition(init_name, init_data_type, init_nullable, true) {}

TableColumnDefinition::TableColumnDefinition(const std::string& init_name, const DataType init_data_type,
                                             const bool init_nullable, const bool init_loaded)
    : name(init_name), data_type(init_data_type), nullable(init_nullable), loaded(init_loaded) {}

bool TableColumnDefinition::operator==(const TableColumnDefinition& rhs) const {
  return name == rhs.name && data_type == rhs.data_type && nullable == rhs.nullable && loaded == rhs.loaded;
}

size_t TableColumnDefinition::hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, name);
  boost::hash_combine(hash, data_type);
  boost::hash_combine(hash, nullable);
  boost::hash_combine(hash, loaded);
  return hash;
}
}  // namespace hyrise
