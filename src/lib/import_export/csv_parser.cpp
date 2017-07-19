#include "csv_parser.hpp"

#include <fstream>
#include <memory>
#include <string>
#include <list>

#include "utils/assert.hpp"
#include "import_export/csv_converter.hpp"
#include "scheduler/job_task.hpp"

// #include "resolve_type.hpp"
// #include "types.hpp"
// #include "utils/assert.hpp"

namespace opossum {

CsvParser::CsvParser(size_t buffer_size, const CsvConfig & csv_config)
    : _buffer_size(buffer_size), _csv_config(csv_config) {}

std::shared_ptr<Table> CsvParser::parse(const std::string& filename) {
  const auto& delimiter = _csv_config.delimiter;

  const auto table = process_meta_file(filename + _csv_config.meta_file_extension);

  std::ifstream csvfile { filename };
  std::string content { std::istreambuf_iterator<char>(csvfile), {} };

  // return empty table if input file is empty
  if (!csvfile) return table;

  // make sure content ends with '\n' for better row processing later
  if (content.back() != delimiter) content.push_back(delimiter);

  // Safe chunks in list to avoid memory relocations
  std::list<Chunk> chunks;
  std::vector<std::shared_ptr<JobTask>> tasks;
  size_t pos;
  std::vector<size_t> row_ends;
  while (!content.empty()) {
    row_ends.clear();
    pos = find_Nth(content, delimiter, table->chunk_size(), row_ends);

    // create chunk and fill with columns
    chunks.emplace_back(true);
    auto& chunk = chunks.back();

    // create and start parsing task
    tasks.emplace_back(std::make_shared<JobTask>([this, &chunk, &table, &content, &row_ends]() {
      parse_chunk(chunk, *table, content, row_ends);
    }));
    tasks.back()->schedule();

    content.erase(0, pos + 1);
  }

  for (auto& task : tasks) {
    task->join();
  }

  for (auto& chunk : chunks) {
    table->add_chunk(std::move(chunk));
  }

  return table;
}

void CsvParser::parse_chunk(Chunk & chunk, const Table & table, const std::string & csvcontent, const std::vector<size_t> & row_ends) {
  size_t start = 0;
  for (const auto end : row_ends) {
    auto row = csvcontent.substr(start, end-start);
    start = end + 1;
  }
}

std::shared_ptr<Table> CsvParser::process_meta_file(const std::string & filename) {
  const char delimiter = '\n';
  const char seperator = ',';

  std::ifstream metafile { filename };
  std::string content { std::istreambuf_iterator<char>(metafile), {} };

  // make sure content ends with '\n' for better row processing later
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

size_t CsvParser::find_Nth(const std::string & str, const char & find, const unsigned int N, std::vector<size_t> & indices) {
  DebugAssert(indices.empty(), "CsvParse::find_Nth should be passed an empty indices vector.");
  if ( 0 == N ) { return std::string::npos; }

  size_t pos, from = 0;
  indices.reserve(N);

  for (unsigned int i = 0; i < N; ++i) {
    pos = str.find(find, from);
    if ( std::string::npos == pos ) { break; }
    indices.push_back(pos);
    from = pos + 1; // from = pos + find.size();
  }

  return pos;
}

}  // namespace opossum
