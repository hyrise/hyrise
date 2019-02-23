#include "load_table.hpp"

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "storage/table.hpp"

#include "constant_mappings.hpp"
#include "string_utils.hpp"

namespace opossum {

std::shared_ptr<Table> create_table_from_header(std::ifstream& infile, size_t chunk_size) {
  std::string line;
  std::getline(infile, line);
  std::vector<std::string> column_names = split_string_by_delimiter(line, '|');
  std::getline(infile, line);
  std::vector<std::string> column_types = split_string_by_delimiter(line, '|');

  auto column_nullable = std::vector<bool>{};
  for (auto& type : column_types) {
    auto type_nullable = split_string_by_delimiter(type, '_');
    type = type_nullable[0];

    auto nullable = type_nullable.size() > 1 && type_nullable[1] == "null";
    column_nullable.push_back(nullable);
  }

  TableColumnDefinitions column_definitions;
  for (size_t i = 0; i < column_names.size(); i++) {
    const auto data_type = data_type_to_string.right.find(column_types[i]);
    Assert(data_type != data_type_to_string.right.end(),
           std::string("Invalid data type ") + column_types[i] + " for column " + column_names[i]);
    column_definitions.emplace_back(column_names[i], data_type->second, column_nullable[i]);
  }

  return std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);
}

std::shared_ptr<Table> create_table_from_header(const std::string& file_name, size_t chunk_size) {
  std::ifstream infile(file_name);
  Assert(infile.is_open(), "load_table: Could not find file " + file_name);
  return create_table_from_header(infile, chunk_size);
}

std::shared_ptr<Table> load_table(const std::string& file_name, size_t chunk_size) {
  std::ifstream infile(file_name);
  Assert(infile.is_open(), "load_table: Could not find file " + file_name);

  auto table = create_table_from_header(infile, chunk_size);

  std::string line;
  while (std::getline(infile, line)) {
    auto string_values = split_string_by_delimiter(line, '|');
    auto variant_values = std::vector<AllTypeVariant>(string_values.size());

    for (auto column_id = ColumnID{0}; column_id < string_values.size(); ++column_id) {
      if (table->column_is_nullable(column_id) && string_values[column_id] == "null") {
        variant_values[column_id] = NULL_VALUE;
      } else {
        variant_values[column_id] = AllTypeVariant{pmr_string{string_values[column_id]}};
      }
    }

    table->append(variant_values);

    auto chunk = table->get_chunk(static_cast<ChunkID>(table->chunk_count() - 1));
    auto mvcc_data = chunk->get_scoped_mvcc_data_lock();
    mvcc_data->begin_cids.back() = 0;
  }
  return table;
}

}  // namespace opossum
