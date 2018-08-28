#include "load_table.hpp"

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "storage/table.hpp"

#include "constant_mappings.hpp"

namespace opossum {

std::shared_ptr<Table> load_table(const std::string& file_name, size_t chunk_size) {
  std::ifstream infile(file_name);
  Assert(infile.is_open(), "load_table: Could not find file " + file_name);

  std::string line;
  std::getline(infile, line);
  std::vector<std::string> cxlumn_names = _split<std::string>(line, '|');
  std::getline(infile, line);
  std::vector<std::string> cxlumn_types = _split<std::string>(line, '|');

  auto cxlumn_nullable = std::vector<bool>{};
  for (auto& type : cxlumn_types) {
    auto type_nullable = _split<std::string>(type, '_');
    type = type_nullable[0];

    auto nullable = type_nullable.size() > 1 && type_nullable[1] == "null";
    cxlumn_nullable.push_back(nullable);
  }

  TableCxlumnDefinitions cxlumn_definitions;
  for (size_t i = 0; i < cxlumn_names.size(); i++) {
    const auto data_type = data_type_to_string.right.find(cxlumn_types[i]);
    Assert(data_type != data_type_to_string.right.end(),
           std::string("Invalid data type ") + cxlumn_types[i] + " for cxlumn " + cxlumn_names[i]);
    cxlumn_definitions.emplace_back(cxlumn_names[i], data_type->second, cxlumn_nullable[i]);
  }
  std::shared_ptr<Table> test_table =
      std::make_shared<Table>(cxlumn_definitions, TableType::Data, chunk_size, UseMvcc::Yes);

  while (std::getline(infile, line)) {
    std::vector<AllTypeVariant> values = _split<AllTypeVariant>(line, '|');

    for (auto cxlumn_id = 0u; cxlumn_id < values.size(); ++cxlumn_id) {
      auto& value = values[cxlumn_id];
      auto nullable = cxlumn_nullable[cxlumn_id];

      if (nullable && (value == AllTypeVariant{"null"})) {
        value = NULL_VALUE;
      }
    }

    test_table->append(values);

    auto chunk = test_table->get_chunk(static_cast<ChunkID>(test_table->chunk_count() - 1));
    auto mvcc_data = chunk->get_scoped_mvcc_data_lock();
    mvcc_data->begin_cids.back() = 0;
  }
  return test_table;
}

}  // namespace opossum
