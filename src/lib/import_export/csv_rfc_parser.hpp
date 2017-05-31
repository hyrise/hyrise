#pragma once

#include <memory>
#include <string>
#include <vector>

#include "storage/table.hpp"

namespace opossum {

/*
 * Creates a Table with values of the parsed csv file <filename> and the corresponding meta file
 * <filename>.meta
 * The files are parsed according to RFC 4180.
 * For the structure of the meta csv file see export_csv.hpp
 *
 * This parser reads the whole csv file and iterates over it to seperate the data into chunks that are aligned with the
 * csv rows.
 * Each data chunk is parsed and converted into a opossum chunk. In the end all chunks are combined to the final table.
 * Currently the data is converted with atoi and family in order to reduce memory allocations. These functions require
 * null terminated strings. That's why we write null bytes to the data chunks in memory. This solution is a little hacky
 * and can be improved when std::from_chars is available.
 */
class CsvRfcParser {
 public:
  /*
   * @param filename    Path to the input file.
   * @param buffer_size  Specifies the amount of data from the input file in bytes that a single task should work on.
   */
  explicit CsvRfcParser(size_t buffer_size);

  // cannot move-assign because of const members
  CsvRfcParser& operator=(CsvRfcParser&&) = delete;

  // Returns the table that was created from the csv file.
  std::shared_ptr<Table> parse(const std::string& filename);

 protected:
  // Number of bytes that a task processes from the input file.
  const size_t _buffer_size;

 protected:
  /* Creates the table structure from the meta file.
   * A meta file contains the chunk size and names and types of each column.
   * This is the content of an example meta file:
   *
   *  PropertyType,Key,Value
   *  Chunk Size,,100
   *  Column Type,a,int
   *  Column Type,b,string
   *  Column Type,c,float
   */
  static const std::shared_ptr<Table> _process_meta_file(const std::string& meta_file);

  /*
   * Processes one part of the input data and fills the given chunk with columns which contain the parsed values.
   * Start and end must point to a valid start/end of a csv row.
   *
   * @param start      Start of the data that should be parsed
   * @param end        End of the data that should be parsed
   * @param chunk      Empty chunk. The result is stored in this chunk.
   * @param table      Table for that the data should be parsed.
   * @param row_count  Number of rows in the chunk from start to end
   */
  static void _parse_file_chunk(alloc_vector<char>::iterator start, alloc_vector<char>::iterator end, Chunk& chunk,
                                const Table& table, ChunkOffset row_count);

  /*
   * Returns the start position of the next csv field after 'start'.
   * Returns 'end' if there is no other field between 'start' and 'end'.
   * Writes a null byte after the end of the csv field that 'start' points to.
   * A null byte is also needed at 'end', it must point to a valid address
   *
   * @param start      Iterator pointing to the begin of a csv field
   * @param end        Search until 'end' for the next field
   * @param last_char  In this parameter the character after the field is saved (separator or delimiter)
   */
  static alloc_vector<char>::iterator _next_field(const alloc_vector<char>::iterator& start,
                                                  const alloc_vector<char>::iterator& end, char& last_char);

  /*
   * Returns the start position of the next csv row after 'start'.
   * Returns 'end' if there is no other row between 'start' and 'end'.
   *
   * @param start      Iterator pointing to the begin of a csv row
   * @param end        Search until 'end' for the next row
   */
  static alloc_vector<char>::iterator _next_row(const alloc_vector<char>::iterator& start,
                                                const alloc_vector<char>::iterator& end);
};
}  // namespace opossum
