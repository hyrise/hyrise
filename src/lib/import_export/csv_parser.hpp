#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common.hpp"
#include "import_export/csv.hpp"

namespace opossum {

class Table;
class Chunk;

/*
 * Creates a Table with values of the parsed csv file <filename> and the corresponding meta file
 * <filename>.meta
 * The files are parsed according to RFC 4180 if not otherwise specified. [https://tools.ietf.org/html/rfc4180]
 * For non-RFC 4180, all linebreaks within quoted strings are further escaped with an escape character.
 * For the structure of the meta csv file see export_csv.hpp
 *
 * This parser reads the whole csv file and iterates over it to seperate the data into chunks that are aligned with the
 * csv rows.
 * Each data chunk is parsed and converted into a opossum chunk. In the end all chunks are combined to the final table.
 */
class CsvParser {
 public:
  /*
   * @param csv_config  Csv configuration (delimiter, separator, ..).
   * @param rfc_mode    Indicator whether RFC 4180 should be used for parsing.
   */
  explicit CsvParser(const CsvConfig& csv_config = {});

  // cannot move-assign because of const members
  CsvParser& operator=(CsvParser&&) = delete;

  /*
   * @param filename Path to the input file.
   * @returns        The table that was created from the csv file.
   */
  std::shared_ptr<Table> parse(const std::string& filename);

 protected:
  /*
   * @param filename Path to the .meta file.
   * @returns        Empty table with column and chunk information based on .meta file.
   */
  std::shared_ptr<Table> _process_meta_file(const std::string& filename);

  /*
   * @param      csv_content String_view on the remaining content of the CSV.
   * @param      table       Empty table created by _process_meta_file.
   * @param[out] field_ends  Empty vector, to be filled with positions of the field ends for one chunk found in \p
   * csv_content.
   * @returns                False if \p csv_content is empty or chunk_size set to 0, True otherwise.
   */
  bool _find_fields_in_chunk(string_view csv_content, const Table& table, std::vector<size_t>& field_ends);

  /*
   * @param      csv_chunk  String_view on one chunk of the CSV.
   * @param      field_ends Positions of the field ends of the given \p csv_chunk.
   * @param      table      Empty table created by _process_meta_file.
   * @param[out] chunk      Empty chunk, to be filled with fields found in \p csv_chunk.
   */
  void _parse_into_chunk(string_view csv_chunk, const std::vector<size_t>& field_ends, const Table& table,
                         Chunk& chunk);

  /*
   * @param field The field that needs to be modified to be RFC 4180 compliant.
   */
  void _sanitize_field(std::string& field);

  // Csv configuration, e.g. delimiter, separator, etc.
  const CsvConfig _csv_config;
};
}  // namespace opossum
