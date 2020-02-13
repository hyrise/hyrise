#include "csv_parser.hpp"

#include <fstream>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_converter.hpp"
#include "import_export/csv/csv_meta.hpp"
#include "resolve_type.hpp"
#include "scheduler/job_task.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"

namespace opossum {

std::shared_ptr<Table> CsvParser::parse(const std::string& filename, const ChunkOffset chunk_size,
                                        const std::optional<CsvMeta>& csv_meta) {
  // If no meta info is given as a parameter, look for a json file
  CsvMeta meta;
  if (csv_meta == std::nullopt) {
    meta = process_csv_meta_file(filename + CsvMeta::META_FILE_EXTENSION);
  } else {
    meta = *csv_meta;
  }

  auto escaped_linebreak = std::string(1, meta.config.delimiter_escape) + std::string(1, meta.config.delimiter);

  auto table = _create_table_from_meta(chunk_size, meta);

  std::ifstream csvfile{filename};

  // return empty table if input file is empty
  if (!csvfile || csvfile.peek() == EOF || csvfile.peek() == '\r' || csvfile.peek() == '\n') return table;

  /**
   * Load the whole file(!) into a std::string using the, hopefully, fastest method to do so.
   * TODO(anybody) Maybe use mmap() in the future. The current approach needs to have the entire file in RAM, mmap might
   *               be cleverer, dunno.
   */
  csvfile.seekg(0, std::ios::end);
  const auto csvfile_size = csvfile.tellg();
  std::string content(csvfile_size, ' ');
  csvfile.seekg(0);
  csvfile.read(content.data(), csvfile_size);

  // make sure content ends with a delimiter for better row processing later
  if (content.back() != meta.config.delimiter) content.push_back(meta.config.delimiter);

  std::string_view content_view{content.c_str(), content.size()};

  // Save chunks in list to avoid memory relocation
  std::list<Segments> segments_by_chunks;
  std::vector<std::shared_ptr<AbstractTask>> tasks;
  std::vector<size_t> field_ends;
  std::mutex append_chunk_mutex;
  while (_find_fields_in_chunk(content_view, *table, field_ends, meta)) {
    // create empty chunk
    segments_by_chunks.emplace_back();
    auto& segments = segments_by_chunks.back();

    // Only pass the part of the string that is actually needed to the parsing task
    std::string_view relevant_content = content_view.substr(0, field_ends.back());

    // Remove processed part of the csv content
    content_view = content_view.substr(field_ends.back() + 1);

    // create and start parsing task to fill chunk
    tasks.emplace_back(std::make_shared<JobTask>([relevant_content, field_ends, &table, &segments, &meta,
                                                  &escaped_linebreak, &append_chunk_mutex]() {
      _parse_into_chunk(relevant_content, field_ends, *table, segments, meta, escaped_linebreak, append_chunk_mutex);
    }));
    tasks.back()->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(tasks);

  for (auto& segments : segments_by_chunks) {
    DebugAssert(!segments.empty(), "Empty chunks shouldn't occur when importing CSV");
    const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
    table->append_chunk(segments, mvcc_data);
  }

  table->last_chunk()->finalize();

  return table;
}

std::shared_ptr<Table> CsvParser::create_table_from_meta_file(const std::string& filename,
                                                              const ChunkOffset chunk_size) {
  const auto meta = process_csv_meta_file(filename);
  return _create_table_from_meta(chunk_size, meta);
}

std::shared_ptr<Table> CsvParser::_create_table_from_meta(const ChunkOffset chunk_size, const CsvMeta& meta) {
  TableColumnDefinitions column_definitions;
  for (const auto& column_meta : meta.columns) {
    auto column_name = column_meta.name;
    BaseCsvConverter::unescape(column_name);

    auto column_type = column_meta.type;
    BaseCsvConverter::unescape(column_type);

    const auto data_type = data_type_to_string.right.at(column_type);

    column_definitions.emplace_back(column_name, data_type, column_meta.nullable);
  }

  return std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);
}

bool CsvParser::_find_fields_in_chunk(std::string_view csv_content, const Table& table, std::vector<size_t>& field_ends,
                                      const CsvMeta& meta) {
  field_ends.clear();
  if (csv_content.empty()) {
    return false;
  }

  std::string search_for{meta.config.separator, meta.config.delimiter, meta.config.quote};

  size_t from = 0;
  unsigned int rows = 0;
  unsigned int field_count = 1;
  bool in_quotes = false;
  while (rows < table.target_chunk_size()) {
    // Find either of row separator, column delimiter, quote identifier
    auto pos = csv_content.find_first_of(search_for, from);
    if (std::string::npos == pos) {
      break;
    }
    from = pos + 1;
    const char elem = csv_content[pos];

    // Make sure to "toggle" in_quotes ONLY if the quotes are not part of the string (i.e. escaped)
    if (elem == meta.config.quote) {
      bool quote_is_escaped = false;
      if (meta.config.quote != meta.config.escape) {
        quote_is_escaped = pos != 0 && csv_content[pos - 1] == meta.config.escape;
      }
      if (!quote_is_escaped) {
        in_quotes = !in_quotes;
      }
    }

    // Determine if delimiter marks end of row or is part of the (string) value
    if (elem == meta.config.delimiter && !in_quotes) {
      DebugAssert(field_count == static_cast<size_t>(table.column_count()),
                  "Number of CSV fields does not match number of columns.");
      ++rows;
      field_count = 0;
    }

    // Determine if separator marks end of field or is part of the (string) value
    if (in_quotes || elem == meta.config.quote) {
      continue;
    }

    ++field_count;
    field_ends.push_back(pos);
  }

  return true;
}

size_t CsvParser::_parse_into_chunk(std::string_view csv_chunk, const std::vector<size_t>& field_ends,
                                    const Table& table, Segments& segments, const CsvMeta& meta,
                                    const std::string& escaped_linebreak, std::mutex& append_chunk_mutex) {
  // For each csv column, create a CsvConverter which builds up a ValueSegment
  const auto column_count = table.column_count();
  const auto row_count = field_ends.size() / column_count;
  std::vector<std::unique_ptr<BaseCsvConverter>> converters;

  for (ColumnID column_id{0}; column_id < column_count; ++column_id) {
    const auto is_nullable = table.column_is_nullable(column_id);
    const auto column_type = table.column_data_type(column_id);

    converters.emplace_back(
        make_unique_by_data_type<BaseCsvConverter, CsvConverter>(column_type, row_count, meta.config, is_nullable));
  }

  Assert(field_ends.size() == row_count * column_count, "Unexpected number of fields");

  size_t start = 0;
  size_t row_id = 0;
  size_t field_idx = 0;
  ColumnID column_id{0};

  try {
    for (; row_id < row_count; ++row_id) {
      for (column_id = ColumnID{0}; column_id < column_count; ++column_id, ++field_idx) {
        const auto end = field_ends[field_idx];
        auto field = std::string{csv_chunk.substr(start, end - start)};
        start = end + 1;

        if (!meta.config.rfc_mode) {
          // CSV fields not following RFC 4810 might need some preprocessing
          _sanitize_field(field, meta, escaped_linebreak);
        }

        converters[column_id]->insert(field, static_cast<ChunkOffset>(row_id));
      }
    }
  } catch (const std::exception& exception) {
    throw std::logic_error("Exception while parsing CSV, row " + std::to_string(row_id) + ", column " +
                           std::to_string(column_id) + ":\n" + exception.what());
  }

  // Transform the field_offsets to segments and add segments to chunk.
  {
    std::lock_guard<std::mutex> lock(append_chunk_mutex);
    for (auto& converter : converters) {
      segments.push_back(converter->finish());
    }
  }

  return row_count;
}

void CsvParser::_sanitize_field(std::string& field, const CsvMeta& meta, const std::string& escaped_linebreak) {
  std::string::size_type pos = 0;
  while ((pos = field.find(escaped_linebreak, pos)) != std::string::npos) {
    field.replace(pos, escaped_linebreak.size(), 1, meta.config.delimiter);
    ++pos;
  }
}

}  // namespace opossum
