#include "csv_parser.hpp"

#include <fstream>
#include <memory>
#include <string>

#include "utils/assert.hpp"
#include "import_export/csv_converter.hpp"
// #include "scheduler/job_task.hpp"

// #include "resolve_type.hpp"
// #include "types.hpp"
// #include "utils/assert.hpp"

namespace opossum {

CsvParser::CsvParser(size_t buffer_size, const CsvConfig& csv_config)
    : _buffer_size(buffer_size), _csv_config(csv_config) {}

std::shared_ptr<Table> CsvParser::parse(const std::string& filename) {
  const auto table = process_meta_file(filename + _csv_config.meta_file_extension);
  return table;
}

std::shared_ptr<Table> CsvParser::process_meta_file(const std::string& filename) {
  const char delimiter = '\n';
  const char seperator = ',';

  std::ifstream metafile { filename };
  std::string content { std::istreambuf_iterator<char>(metafile), std::istreambuf_iterator<char>() };

  // make sure content ends with '\n' for better row processing
  if (content.back() != delimiter) content.push_back(delimiter);

  // skip header
  content.erase(0, content.find(delimiter) + 1);

  // skip next two fields
  content.erase(0, content.find(seperator) + 1);
  content.erase(0, content.find(seperator) + 1);

  // read chunk size
  auto pos = content.find(delimiter);
  const size_t chunk_size { std::stoul(content.substr(0, pos)) };
  content.erase(0, pos + 1);

  const auto table = std::make_shared<Table>(chunk_size);

  //read column info
  while ((pos = content.find(delimiter)) != std::string::npos) {
    auto row = content.substr(0, pos);

    // skip field
    row.erase(0, row.find(seperator) + 1);

    // read column name
    auto row_pos = row.find(seperator);
    const auto column_name = row.substr(0, row_pos);
    row.erase(0, row_pos + 1);

    // read column type
    row_pos = row.find(seperator);
    const auto column_type = row.substr(0, row_pos);

    content.erase(0, pos + 1);

    table->add_column_definition(column_name, column_type);
  }

  return table;
}

}  // namespace opossum
