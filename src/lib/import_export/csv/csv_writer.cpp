#include "csv_writer.hpp"

#include <fstream>
#include <string>
#include <vector>
#include "types.hpp"

namespace opossum {

void CsvWriter::write(const Table& table, const std::string& filename, const ParseConfig& config) {
  _generate_meta_info_file(table, filename + CsvMeta::META_FILE_EXTENSION);
  _generate_content_file(table, filename, config);
}

void CsvWriter::_generate_meta_info_file(const Table& table, const std::string& filename) {
  CsvMeta meta{};

  // Column Types
  for (ColumnID column_id{0}; column_id < table.column_count(); ++column_id) {
    ColumnMeta column_meta;
    column_meta.name = table.column_name(column_id);
    column_meta.type = data_type_to_string.left.at(table.column_data_type(column_id));
    column_meta.nullable = table.column_is_nullable(column_id);

    meta.columns.push_back(column_meta);
  }

  nlohmann::json meta_json = meta;

  std::ofstream meta_file_stream(filename);
  meta_file_stream << std::setw(4) << meta_json << std::endl;
}

void CsvWriter::_generate_content_file(const Table& table, const std::string& filename, const ParseConfig& config) {
  /**
   * A naively exported csv file is a materialized file in row format.
   * This offers some advantages, but also disadvantages.
   * The advantages are that it is very straight forward to implement for any segment type
   * as it does not care about representation of values. Also, probably, the main reason for this,
   * it makes is very easy to load this data into a different database.
   * The disadvantage is that it can be quite slow if the data has been compressed before.
   * Also, it does not involve the column-oriented style used in OpossumDB.
   */

  // Open file for writing
  std::ofstream ofstream;
  ofstream.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  ofstream.open(filename);

  /**
   * Multiple rows containing the values of each respective row are written.
   * Therefore we first iterate through the chunks, then through the rows
   * in the chunks and afterwards through the segments of the chunks.
   *
   * This is a lot of iterating, but to convert a column-based table to
   * a row-based representation takes some effort.
   */
  const auto chunk_count = table.chunk_count();
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table.get_chunk(chunk_id);
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); ++chunk_offset) {
      for (ColumnID column_id{0}; column_id < table.column_count(); ++column_id) {
        const auto segment = chunk->get_segment(column_id);

        // The previous implementation did a double dispatch (at least two virtual method calls)
        // So the subscript operator cannot be much slower.
        const auto value = (*segment)[chunk_offset];
        if (column_id != ColumnID{0}) {
          ofstream << config.separator;
        }
        _write(value, ofstream, config);
      }

      ofstream << config.delimiter;
    }
  }

  ofstream.close();
}

void CsvWriter::_write(const AllTypeVariant& value, std::ofstream& ofstream, const ParseConfig& config) {
  if (variant_is_null(value)) return;

  if (value.type() == typeid(pmr_string)) {
    _write_string_value(boost::get<pmr_string>(value), ofstream, config);
    return;
  }

  ofstream << value;
}

void CsvWriter::_write_string_value(const pmr_string& value, std::ofstream& ofstream, const ParseConfig& config) {
  /**
   * We put the quotechars around any string value by default
   * as this is the only time when a comma (,) might be inside a value.
   * This might consume more space, however it speeds the program as it
   * does not require additional checks.
   * If we start allowing more characters as delimiter, we should change
   * this behaviour to either general quoting or checking for "illegal"
   * characters.
   */
  ofstream << config.quote;
  ofstream << _escape(value, config);
  ofstream << config.quote;
}

/*
 * Escapes each quote character with an escape symbol.
 */
pmr_string CsvWriter::_escape(const pmr_string& string, const ParseConfig& config) {
  pmr_string result(string);
  size_t next_pos = 0;
  while (std::string::npos != (next_pos = result.find(config.quote, next_pos))) {
    result.insert(next_pos, 1, config.escape);
    // Has to jump 2 positions ahead because a new character had been inserted.
    next_pos += 2;
  }
  return result;
}

}  // namespace opossum
