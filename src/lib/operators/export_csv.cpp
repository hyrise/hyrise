#include "export_csv.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "json.hpp"

#include "import_export/csv_meta.hpp"
#include "import_export/csv_writer.hpp"
#include "storage/materialize.hpp"
#include "storage/reference_column.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"

namespace opossum {

ExportCsv::ExportCsv(const std::shared_ptr<const AbstractOperator>& in, const std::string& filename)
    : AbstractReadOnlyOperator(OperatorType::ExportCsv, in), _filename(filename) {}

const std::string ExportCsv::name() const { return "ExportCSV"; }

std::shared_ptr<const Table> ExportCsv::_on_execute() {
  _generate_meta_info_file(_input_left->get_output(), _filename + CsvMeta::META_FILE_EXTENSION);
  _generate_content_file(_input_left->get_output(), _filename);

  return _input_left->get_output();
}

std::shared_ptr<AbstractOperator> ExportCsv::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<ExportCsv>(copied_input_left, _filename);
}

void ExportCsv::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void ExportCsv::_generate_meta_info_file(const std::shared_ptr<const Table>& table, const std::string& meta_file_path) {
  CsvMeta meta{};
  meta.chunk_size = table->max_chunk_size();

  // Column Types
  for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
    ColumnMeta column_meta;
    column_meta.name = table->column_name(column_id);
    column_meta.type = data_type_to_string.left.at(table->column_data_type(column_id));
    column_meta.nullable = table->column_is_nullable(column_id);

    meta.columns.push_back(column_meta);
  }

  nlohmann::json meta_json = meta;

  std::ofstream meta_file_stream(meta_file_path);
  meta_file_stream << std::setw(4) << meta_json << std::endl;
}

void ExportCsv::_generate_content_file(const std::shared_ptr<const Table>& table, const std::string& csv_file) {
  /**
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

  /**
   * Multiple rows containing the values of each respective row are written.
   * Therefore we first iterate through the chunks, then through the rows
   * in the chunks and afterwards through the columns of the chunks.
   *
   * This is a lot of iterating, but to convert a column-based table to
   * a row-based representation takes some effort.
   */
  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);

    for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); ++chunk_offset) {
      for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
        const auto column = chunk->get_column(column_id);

        // The previous implementation did a double dispatch (at least two virtual method calls)
        // So the subscript operator cannot be much slower.
        const auto value = (*column)[chunk_offset];
        writer.write(value);
      }

      writer.end_line();
    }
  }
}

}  // namespace opossum
