#include "csv_parser.hpp"

#include <fstream>
#include <memory>
#include <string>
#include <list>

#include "utils/assert.hpp"
#include "import_export/csv_converter.hpp"
#include "scheduler/job_task.hpp"

#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

CsvParser::CsvParser(size_t buffer_size, const CsvConfig & csv_config)
    : _buffer_size(buffer_size), _csv_config(csv_config) {}

std::shared_ptr<Table> CsvParser::parse(const std::string& filename) {
  const auto table = process_meta_file(filename + _csv_config.meta_file_extension);

  std::ifstream csvfile { filename };
  std::string content { std::istreambuf_iterator<char>(csvfile), {} };

  // return empty table if input file is empty
  if (!csvfile) return table;

  // make sure content ends with a delimiter for better row processing later
  if (content.back() != _csv_config.delimiter) content.push_back(_csv_config.delimiter);

  // Safe chunks in list to avoid memory relocations
  std::list<Chunk> chunks;
  std::vector<std::shared_ptr<JobTask>> tasks;
  std::vector<size_t> field_ends;
  while (find_fields_in_chunk(content, *table.get(), field_ends)) {

    // create empty chunk
    chunks.emplace_back(true);
    auto& chunk = chunks.back();

    // create and start parsing task to fill chunk
    tasks.emplace_back(std::make_shared<JobTask>([this, &content, &table, &field_ends, &chunk]() {
      parse_into_chunk(content, *table, field_ends, chunk);
    }));
    tasks.back()->schedule();

    // Remove processed part of the csv content
    content.erase(0, field_ends.back() + 1);
  }

  for (auto& task : tasks) {
    task->join();
  }

  for (auto& chunk : chunks) {
    table->add_chunk(std::move(chunk));
  }

  return table;
}

std::shared_ptr<Table> CsvParser::process_meta_file(const std::string & filename) {
  const char delimiter = '\n';
  const char separator = ',';

  std::ifstream metafile { filename };
  std::string content { std::istreambuf_iterator<char>(metafile), {} };

  // make sure content ends with '\n' for better row processing later
  if (content.back() != delimiter) content.push_back(delimiter);

  // skip header
  content.erase(0, content.find(delimiter) + 1);

  // skip next two fields
  content.erase(0, content.find(separator) + 1);
  content.erase(0, content.find(separator) + 1);

  // read chunk size
  auto pos = content.find(delimiter);
  const size_t chunk_size { std::stoul(content.substr(0, pos)) };
  content.erase(0, pos + 1);

  const auto table = std::make_shared<Table>(chunk_size);

  //read column info
  while ((pos = content.find(delimiter)) != std::string::npos) {
    auto row = content.substr(0, pos);

    // skip field
    row.erase(0, row.find(separator) + 1);

    // read column name
    auto row_pos = row.find(separator);
    auto column_name = row.substr(0, row_pos);
    AbstractCsvConverter::unescape(column_name);
    row.erase(0, row_pos + 1);

    // read column type
    row_pos = row.find(separator);
    auto column_type = row.substr(0, row_pos);
    AbstractCsvConverter::unescape(column_type);

    content.erase(0, pos + 1);
    table->add_column_definition(column_name, column_type);
  }

  return table;
}

bool CsvParser::find_fields_in_chunk(const std::string & str, const Table & table, std::vector<size_t> & indices) {
  indices.clear();
  if ( 0 == table.chunk_size() || str.empty()) { return false; }

  std::string search_for {_csv_config.separator, _csv_config.delimiter, _csv_config.quote};

  size_t pos, from = 0;
  unsigned int rows = 0, field_count = 1;
  bool in_quotes = false;
  while (rows < table.chunk_size()) {
    // Find either of row separator, column delimitor, quote identifier
    pos = str.find_first_of(search_for, from);
    if ( std::string::npos == pos ) { break; }
    from = pos + 1;
    const char elem = str.at(pos);

    in_quotes = (elem == _csv_config.quote) ? !in_quotes : in_quotes;

    // Determine if delimiter marks end of row or is part of the (string) value
    if ( elem == _csv_config.delimiter && !in_quotes) {
      Assert(field_count == table.col_count(), "Number of CSV fields does not match number of columns.");
      ++rows;
      field_count = 0;
    }

    // Determine if separator marks end of field or is part of the (string) value
    if (in_quotes || elem == _csv_config.quote) { continue; }

    ++field_count;
    indices.push_back(pos);
  }

  return true;
}

void CsvParser::parse_into_chunk(const std::string & content, const Table & table, const std::vector<size_t> & field_ends, Chunk & chunk) {
  // For each csv column create a CsvConverter which builds up a ValueColumn
  const auto col_count = table.col_count();
  const auto row_count = field_ends.size() / col_count;
  std::vector<std::unique_ptr<AbstractCsvConverter>> converters;
  for (ColumnID column_id{0}; column_id < col_count; ++column_id) {
    converters.emplace_back(
        make_unique_by_column_type<AbstractCsvConverter, CsvConverter>(table.column_type(column_id), row_count, _csv_config));
  }

  size_t start = 0;
  for (ChunkOffset row_i = 0; row_i < row_count; ++row_i)
  {
    for (ColumnID col_i{0}; col_i < col_count; ++col_i)
    {
      const auto end = field_ends.at(row_i*col_count + col_i);
      auto field = content.substr(start, end-start);
      start = end + 1;

      converters[col_i]->insert(field, row_i);
    }
  }

  // Transform the field_offsets to columns and add columns to chunk.
  for (auto& converter : converters) {
    chunk.add_column(converter->finish());
  }
}

}  // namespace opossum
