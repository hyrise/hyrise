#include "csv_rfc_parser.hpp"

#include <algorithm>
#include <array>
#include <fstream>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "import_export/csv_converter.hpp"
#include "scheduler/job_task.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

CsvRfcParser::CsvRfcParser(size_t buffer_size, const CsvConfig& csv_config)
    : _buffer_size(buffer_size), _csv_config(csv_config) {}

std::shared_ptr<Table> CsvRfcParser::parse(const std::string& filename) {
  // Use default config for meta file parsing
  auto meta_file_config = CsvConfig{};
  auto table = _process_meta_file(filename + _csv_config.meta_file_extension, meta_file_config);

  std::ifstream file;
  file.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  file.open(filename, std::ifstream::binary | std::ifstream::ate);
  const size_t file_size = file.tellg();
  file.seekg(0);

  // Return empty table if input file is empty.
  if (file_size == 0) return table;

  // Add additional null byte at the end in order to allow stroi and friends to work on a c string that ends on a null
  // byte
  std::vector<char> file_content(file_size + 1);
  file.read(file_content.data(), file_size);

  // Safe chunks in list to avoid memory relocations
  std::list<Chunk> chunks;
  std::vector<std::shared_ptr<JobTask>> tasks;

  auto position = file_content.begin();
  // Use the end without the additional null byte
  const auto content_end = file_content.end() - 1;

  // Loop over file_content, split it into row aligned chunks and start a task for each chunk
  while (position < content_end) {
    ChunkOffset row_count = 0;
    const auto start = position;
    // loop over rows until block size reached or end of file_content reached
    while (position < content_end && position - start < static_cast<std::streamsize>(_buffer_size)) {
      position = _next_row(position, content_end, _csv_config);
      ++row_count;
    }

    // create chunk and fill with columns
    chunks.emplace_back(true);
    auto& chunk = chunks.back();

    const CsvConfig config = _csv_config;
    // create and start parsing task
    tasks.emplace_back(std::make_shared<JobTask>([start, position, &chunk, &table, row_count, config]() {
      _parse_file_chunk(start, position, chunk, *table, row_count, config);
    }));
    tasks.back()->schedule();
  }

  for (auto& task : tasks) {
    task->join();
  }

  for (auto& chunk : chunks) {
    table->add_chunk(std::move(chunk));
  }

  return table;
}

void CsvRfcParser::_parse_file_chunk(std::vector<char>::iterator start, std::vector<char>::iterator end, Chunk& chunk,
                                     const Table& table, ChunkOffset row_count, const CsvConfig& config) {
  if (start == end) return;
  auto position = start;

  // For each csv column create a CsvConverter which builds up a ValueColumn
  std::vector<std::unique_ptr<AbstractCsvConverter>> converters;
  for (ColumnID column_id{0}; column_id < table.col_count(); ++column_id) {
    converters.emplace_back(
        make_unique_by_column_type<AbstractCsvConverter, CsvConverter>(table.column_type(column_id), row_count));
  }

  ColumnID current_column{0};
  ChunkOffset current_row = 0;

  while (position < end) {
    const auto field_start = position;
    char last_char;
    position = _next_field(position, end, last_char, config);
    // _next_field added a null byte to the end of the field. Now the null terminated string can be converted by the
    // CsvConverter
    converters[current_column]->insert(&*field_start, current_row);

    ++current_column;
    // reset current_column if we hit a delimiter
    if (position == end || last_char == config.delimiter) {
      Assert((current_column == table.col_count()), "CSV row does not contain enough values.");
      ++current_row;
      current_column = 0;
    }
    Assert((current_column < table.col_count()), "CSV row contains too many values.");
  }

  // Transform the field_offsets to columns and add columns to chunk.
  for (auto& converter : converters) {
    chunk.add_column(converter->finish());
  }
}

const std::shared_ptr<Table> CsvRfcParser::_process_meta_file(const std::string& meta_file, const CsvConfig& config) {
  std::ifstream file;
  file.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  file.open(meta_file, std::ifstream::binary | std::ifstream::ate);
  const size_t file_size = file.tellg();
  file.seekg(0);

  // reserve one extra slot that can be overwritten to a null byte
  std::vector<char> file_content(file_size + 1);
  file.read(file_content.data(), file_size);

  // ignore additional null byte
  const auto end = file_content.end() - 1;
  char last_char;

  // skip header
  auto position = std::find(file_content.begin(), end, config.delimiter);
  if (position != end) ++position;

  // skip next two fields
  position = _next_field(position, end, last_char, config);
  position = _next_field(position, end, last_char, config);

  auto field_start = position;
  position = _next_field(position, end, last_char, config);
  const size_t chunk_size{std::stoul(field_start.base())};

  const auto table = std::make_shared<Table>(chunk_size);

  // read column info
  while (position < end) {
    // ignore first field
    position = _next_field(position, end, last_char, config);

    field_start = position;
    position = _next_field(position, end, last_char, config);
    std::string column_name = field_start.base();
    AbstractCsvConverter::unescape(column_name, config);

    field_start = position;
    position = _next_field(position, end, last_char, config);
    std::string column_type = field_start.base();
    AbstractCsvConverter::unescape(column_type, config);
    table->add_column(column_name, column_type);
  }
  return table;
}

std::vector<char>::iterator CsvRfcParser::_next_field(const std::vector<char>::iterator& start,
                                                      const std::vector<char>::iterator& end, char& last_char,
                                                      const CsvConfig& config) {
  if (start == end) return start;
  auto position = start;

  if (*position == config.escape) {
    // The field is escaped and we must find the next config.quote that is not followed by another config.quote
    do {
      position = std::find(position + 1, end, config.quote);
      Assert((position != end), "CSV field does not end properly");
      ++position;
    } while (position != end && *position == config.quote);
    Assert(!(position != end && *position != config.separator && *position != config.delimiter), "CSV file is corrupt");
  } else /* field is not escaped */ {
    const std::array<char, 2> search_values = {{config.separator, config.delimiter}};
    position = std::find_first_of(start, end, search_values.begin(), search_values.end());
  }

  // At this point 'position' points to the character after the field. It must be a separator or delimiter or 'end'
  // Save this character because it will be overwritten and we need it later.
  last_char = *position;
  // overwrite with null byte
  *position = 0;
  // jump over delimiter/separator
  if (position < end) ++position;
  return position;
}

std::vector<char>::iterator CsvRfcParser::_next_row(const std::vector<char>::iterator& start,
                                                    const std::vector<char>::iterator& end, const CsvConfig& config) {
  bool is_escaped = false;
  auto position = start;
  // find the next delimiter that is not surrounded by quotes
  while (position < end && (is_escaped || *position != config.delimiter)) {
    if (*position == config.quote) is_escaped = !is_escaped;
    ++position;
  }
  // jump over delimiter
  if (position < end) ++position;
  return position;
}

}  // namespace opossum
