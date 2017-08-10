#include "load_table.hpp"

#include <fstream>
#include <string>

#include "storage/table.hpp"

namespace opossum {

std::shared_ptr<Table> load_table(const std::string &file_name, size_t chunk_size) {
  std::ifstream infile(file_name);
  Assert(infile.is_open(), "load_table: Could not find file ");

  std::string line;
  std::getline(infile, line);
  std::vector<std::string> col_names = _split<std::string>(line, '|');
  std::getline(infile, line);
  std::vector<std::string> col_types = _split<std::string>(line, '|');

  auto col_nullable = std::vector<bool>{};
  for (auto &type : col_types) {
    auto type_nullable = _split<std::string>(type, '_');
    type = type_nullable[0];

    auto nullable = type_nullable.size() > 1 && type_nullable[1] == "null";
    col_nullable.push_back(nullable);
  }

  std::shared_ptr<Table> test_table = std::make_shared<Table>(chunk_size);
  for (size_t i = 0; i < col_names.size(); i++) {
    test_table->add_column(col_names[i], col_types[i], col_nullable[i]);
  }

  while (std::getline(infile, line)) {
    std::vector<AllTypeVariant> values = _split<AllTypeVariant>(line, '|');

    for (auto column_id = 0u; column_id < values.size(); ++column_id) {
      auto &value = values[column_id];
      auto nullable = col_nullable[column_id];

      if (nullable && (value == AllTypeVariant{"null"})) {
        value = NULL_VALUE;
      }
    }

    test_table->append(values);

    auto &chunk = test_table->get_chunk(static_cast<ChunkID>(test_table->chunk_count() - 1));
    auto mvcc_cols = chunk.mvcc_columns();
    mvcc_cols->begin_cids.back() = 0;
  }
  return test_table;
}

}  // namespace opossum
