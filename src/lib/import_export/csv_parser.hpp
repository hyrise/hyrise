#pragma once

#include <memory>
#include <string>

#include "import_export/csv.hpp"
#include "storage/table.hpp"

namespace opossum {

/*
 * Creates a Table with values of the parsed csv file <filename> and the corresponding meta file
 * <filename>.meta
 * The files are parsed according to RFC 4180 if not otherwise specified.
 * For the structure of the meta csv file see export_csv.hpp
 *
 * This parser reads the whole csv file and iterates over it to seperate the data into chunks that are aligned with the
 * csv rows.
 * Each data chunk is parsed and converted into a opossum chunk. In the end all chunks are combined to the final table.
 */
class CsvParser {
 public:
  /*
   * @param buffer_size Number of bytes that a task processes from the input file.
   * @param csv_config  Csv configuration (delimiter, separator, ..).
   * @param rfc_mode    Indicator whether RFC 4180 should be used for parsing.
   */
  explicit CsvParser(const size_t buffer_size, const CsvConfig & csv_config = {}, const bool rfc_mode = true);

  // cannot move-assign because of const members
  CsvParser& operator=(CsvParser &&) = delete;

  /*
   * @param filename Path to the input file.
   * @returns        The table that was created from the csv file.
   */
  std::shared_ptr<Table> parse(const std::string & filename);

 protected:
  std::shared_ptr<Table> process_meta_file(const std::string & filename);
  bool find_fields_in_chunk(const std::string & str, const Table & table, std::vector<size_t> & indices);
  void parse_into_chunk(const std::string & content, const std::vector<size_t> & field_ends, const Table & table, Chunk & chunk);
  void sanitize_field(std::string & field);

  // Number of bytes that a task processes from the input file.
  const size_t _buffer_size;
  // Csv configuration, e.g. delimiter, separator, etc.
  const CsvConfig _csv_config;
  // Indicator whether RFC 4180 should be used for parsing. https://tools.ietf.org/html/rfc4180
  bool _rfc;
};
}  // namespace opossum
