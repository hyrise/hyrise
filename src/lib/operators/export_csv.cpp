#include "export_csv.hpp"

#include <memory>
#include <string>

#include "csv.hpp"
#include "csv_writer.hpp"

namespace opossum {

ExportCsv::ExportCsv(const std::shared_ptr<const AbstractOperator> in, const std::string &directory,
                     const std::string &filename)
    : AbstractOperator(in), _directory(directory), _filename(filename) {}

const std::string ExportCsv::name() const { return "ExportCSV"; }
uint8_t ExportCsv::num_in_tables() const { return 1; }
uint8_t ExportCsv::num_out_tables() const { return 1; }

std::shared_ptr<const Table> ExportCsv::on_execute() {
  _generate_meta_info_file(_input_left->get_output(), _directory + '/' + _filename + csv::meta_file_extension);
  _generate_content_file(_input_left->get_output(), _directory + '/' + _filename + csv::file_extension);

  return _input_left->get_output();
}

void ExportCsv::_generate_meta_info_file(const std::shared_ptr<const Table> &table, const std::string &meta_file) {
  CsvWriter writer(meta_file);
  // Write header line
  writer.write_line({"PropertyType", "Key", "Value"});

  // Chunk size
  writer.write_line({"ChunkSize", "", static_cast<int64_t>(table->chunk_size())});

  // Column Types
  for (size_t col_id = 0u; col_id < table->col_count(); col_id++) {
    writer.write_line({"ColumnType", table->column_name(col_id), table->column_type(col_id)});
  }
}

void ExportCsv::_generate_content_file(const std::shared_ptr<const Table> &table, const std::string &csv_file) {
  /*
   * A naively exported csv file is a materialized file in row format.
   * This offers some advantages, but also disadvantages.
   * The advantages are that it is very straight forward to implement for any column type
   * as it does not care about representation of values. Also, probably, the main reason for this,
   * it makes is very easy to load this data into a different database.
   * The disadvantage is that it can be quite slow if the data has been compressed before.
   * Also, it does not involve the column-oriented style used in OpossumDB.
  */

  // Open file for writing
  CsvWriter writer(csv_file);
  /* Multiple rows containing the values of each respective row are written.
   * Therefore we first iterate through the chunks, then through the rows in the chunks
   * and afterwards through the columns of the chunks.
   * This is a lot of iterating, but to convert a column-based table to a row-based representation
   * takes some effort.
   */
  for (ChunkID chunk_id = 0; chunk_id < table->chunk_count(); chunk_id++) {
    auto &chunk = table->get_chunk(chunk_id);
    for (ChunkOffset row = 0; row < chunk.size(); row++) {
      for (ColumnID col_id = 0; col_id < table->col_count(); col_id++) {
        writer.write(chunk.get_column(col_id)->operator[](row));
      }
      writer.end_line();
    }
  }
}

}  // namespace opossum
