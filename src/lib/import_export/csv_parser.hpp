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
   * @param buffer_size Specifies the amount of data from the input file in bytes that a single task should work on.
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
  void parse_chunk(Chunk & chunk, const Table & table, const std::string & csvcontent, const std::vector<size_t> & row_ends);
  std::shared_ptr<Table> process_meta_file(const std::string & filename);
  size_t find_Nth(const std::string & str, const char & find, const unsigned int N, std::vector<size_t> & indices);

  // Number of bytes that a task processes from the input file.
  const size_t _buffer_size;
  // Csv configuration, e.g. delimiter, separator, etc.
  const CsvConfig _csv_config;
};
}  // namespace opossum
