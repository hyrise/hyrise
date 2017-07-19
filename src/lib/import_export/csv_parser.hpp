#pragma once

#include <memory>
#include <string>

#include "import_export/csv.hpp"
#include "storage/table.hpp"

namespace opossum {

/*
 * Blabla
 */
class CsvParser {
 public:
  /*
   * @param csv_config  Csv configuration (delimiter, separator, ..)
   */
  explicit CsvParser(size_t buffer_size, const CsvConfig & csv_config = {});

  // cannot move-assign because of const members
  CsvParser& operator=(CsvParser &&) = delete;

  /*
   * @param filename Path to the input file.
   * @returns        The table that was created from the csv file.
   */
  std::shared_ptr<Table> parse(const std::string & filename);

 protected:
  std::shared_ptr<Table> process_meta_file(const std::string & filename);
  void parse_into_chunk(const std::string & content, const Table & table, const std::vector<size_t> & field_ends, Chunk & chunk);
  bool find_fields_in_chunk(const std::string & str, const Table & table, std::vector<size_t> & indices);

  // Number of bytes that a task processes from the input file.
  const size_t _buffer_size;
  // Csv configuration, e.g. delimiter, separator, etc.
  const CsvConfig _csv_config;
};
}  // namespace opossum
